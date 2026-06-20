import logging
import os
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd

logging.basicConfig(level=logging.WARNING, format='%(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="大数据分析看板 API",
    version="1.0.0",
    description="Milestone4 前后端分离数据看板后端接口"
)

# ------------------------------
# 防御性设计原则：DuckDB 只读连接
# 本系统暂未使用 DuckDB，若后续引入数据库查询，
# 必须使用 conn = duckdb.connect(database="...", read_only=True)
# 以防止与流式写入 Worker 进程发生写锁冲突。
# ------------------------------

FEATURES_PATH = "../../lab10/batch_1000_features.csv"
RAW_PATH = "../../lab09/online_shopping_10_cats.csv"

DATA_SOURCE = None
LLM_ACTIVE = False


def _generate_sample_data():
    categories = ["手机", "电脑", "家电"]
    sample_reviews = {
        "手机": [
            "手机性能很好，运行流畅",
            "屏幕显示效果出色",
            "电池续航有点短",
            "拍照效果非常好",
            "价格偏高，性价比一般",
            "系统更新后更流畅了",
            "指纹解锁反应灵敏",
            "充电速度很快",
            "外观设计很漂亮",
            "信号接收能力不错",
        ],
        "电脑": [
            "运行速度很快，办公够用",
            "散热效果一般",
            "键盘手感不错",
            "屏幕分辨率很高",
            "电池续航能力强",
            "重量偏重，携带不方便",
            "接口种类丰富",
            "系统预装软件太多",
            "性价比很高",
            "售后服务态度好",
        ],
        "家电": [
            "送货安装很及时",
            "噪音控制很好",
            "节能效果明显",
            "操作界面友好",
            "外观时尚大方",
            "清洁效果不错",
            "价格实惠",
            "功能齐全",
            "安装师傅专业",
            "使用体验很好",
        ],
    }
    records = []
    for cat in categories:
        for i, review in enumerate(sample_reviews[cat]):
            sentiment = "正面" if i < 7 else "负面"
            label = 1 if i < 7 else 0
            records.append({
                "cat": cat,
                "review": review,
                "sentiment": sentiment,
                "label": label,
            })
    return pd.DataFrame(records)


def _load_data():
    global DATA_SOURCE, df
    try:
        if os.path.exists(FEATURES_PATH):
            df = pd.read_csv(FEATURES_PATH, encoding="utf-8-sig")
            DATA_SOURCE = "llm_enhanced"
            print(f"✅ 已加载LLM增强数据: {len(df)} 条")
        elif os.path.exists(RAW_PATH):
            df = pd.read_csv(RAW_PATH, encoding="utf-8-sig")
            df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})
            DATA_SOURCE = "raw_fallback"
            logger.warning("⚠️ [数据降级] LLM增强数据缺失，已回退至原始数据集。")
            print(f"✅ 已加载原始数据: {len(df)} 条")
        else:
            df = _generate_sample_data()
            DATA_SOURCE = "sample_generated"
            logger.warning("⚠️ [数据降级] 所有数据源文件缺失，已自动生成样本数据以维持系统运行。请检查 lab10/batch_1000_features.csv 和 lab09/online_shopping_10_cats.csv 路径。")
            print(f"✅ 已生成样本数据: {len(df)} 条")
    except Exception as e:
        logger.warning(f"⚠️ [数据降级] 数据文件读取失败 ({e})，已自动生成样本数据。")
        df = _generate_sample_data()
        DATA_SOURCE = "sample_generated"
        print(f"✅ 已生成样本数据: {len(df)} 条")


_load_data()

api_key = os.getenv("SILICONFLOW_API_KEY")
if api_key:
    LLM_ACTIVE = True
else:
    logger.warning("⚠️ [LLM降级] SILICONFLOW_API_KEY 环境变量未配置，大模型功能已降级为内置规则库。请设置环境变量以启用完整LLM功能。")


def filter_dataframe(cat: str = None, sentiment: str = None, base_df=None):
    filtered = base_df if base_df is not None else df
    if cat:
        filtered = filtered[filtered["cat"] == cat]
    if sentiment:
        filtered = filtered[filtered["sentiment"] == sentiment]
    return filtered


@app.get("/api/health", summary="服务健康检查", description="验证后端服务是否正常运行")
def health_check():
    return {
        "status": "ok",
        "message": "大数据分析看板后端服务运行正常",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/system-status", summary="系统运行状态", description="返回LLM功能状态、数据源状态等系统运行信息")
def get_system_status():
    llm_reason = "API Key已配置，LLM功能正常" if LLM_ACTIVE else "API_KEY_MISSING"
    data_source_map = {
        "llm_enhanced": "llm_enhanced",
        "raw_fallback": "raw_fallback",
        "sample_generated": "sample_generated",
    }
    return {
        "status": "running",
        "llm_active": LLM_ACTIVE,
        "reason": llm_reason,
        "data_source": data_source_map.get(DATA_SOURCE, "unknown"),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/category-distribution", summary="获取商品品类分布", description="返回各品类的样本数量，供前端柱状图/饼图使用")
def get_category_distribution(sentiment: str = None):
    filtered = filter_dataframe(sentiment=sentiment)
    stats = filtered["cat"].value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist()
    }


@app.get("/api/sentiment-overview", summary="获取各品类情感分布", description="返回各品类下正面/负面/中性情感的数量，供前端堆叠柱状图使用")
def get_sentiment_overview(cat: str = None):
    filtered = filter_dataframe(cat=cat)
    pivot = filtered.groupby(["cat", "sentiment"]).size().unstack(fill_value=0)
    if pivot.empty:
        return {"data": []}
    result = []
    for cat in pivot.index:
        result.append({
            "category": cat,
            **{col: int(pivot.loc[cat, col]) for col in pivot.columns}
        })
    return {"data": result}


@app.get("/api/sub-category-stats", summary="获取指定品类的问题类型分布", description="根据品类统计子维度数量，供柱状图二级下钻使用")
def get_sub_category_stats(cat: str = None):
    if not cat:
        return {"categories": [], "counts": []}
    filtered = filter_dataframe(cat=cat)
    if filtered.empty:
        return {"categories": [], "counts": []}
    columns = filtered.columns
    if "category" in columns:
        print("✅ 使用category作为下钻维度（问题类型）")
        sub_dim = "category"
    elif "issue_type" in columns:
        print("✅ 使用issue_type作为下钻维度")
        sub_dim = "issue_type"
    else:
        print("⚠️  未检测到问题类型字段，自动回退使用sentiment作为下钻维度")
        sub_dim = "sentiment"
    stats = filtered[sub_dim].dropna().value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist()
    }


@app.get("/api/word-cloud", summary="获取词云图词频数据", description="根据品类/情感/关键词筛选后，按中文单字分词统计词频")
def get_word_cloud(cat: str = None, sentiment: str = None, query: str = None):
    filtered = filter_dataframe(cat=cat, sentiment=sentiment)
    if query:
        filtered = filtered[filtered["review"].notna()]
        try:
            filtered = filtered[
                filtered["review"].str.contains(query, case=False, regex=True)
            ]
        except Exception:
            filtered = filtered[
                filtered["review"].str.contains(query, case=False, regex=False)
            ]
    if filtered.empty:
        return []
    if "summary" in filtered.columns:
        texts = filtered["summary"].fillna("") + filtered["review"].fillna("")
    else:
        texts = filtered["review"].fillna("")
    import re
    stopwords = {
        "的","了","是","我","你","他","她","它","在","有","和",
        "都","也","还","很","太","真","好","不","没","就",
        "才","这","那","个","些","一","二","三","四","五","六",
        "七","八","九","十",
        "非常","比较","有点","还是","但是","而且","因为","所以","可以",
        "其实","感觉","觉得","真的","不错","很好","非常好","一般般","不好","太差了"
    }
    from collections import Counter
    word_counter = Counter()
    for text in texts:
        words = re.findall(r'[\u4e00-\u9fa5]{2,}', str(text))
        for w in words:
            if w not in stopwords:
                word_counter[w] += 1
    top_words = word_counter.most_common(50)
    return [{"name": word, "value": count} for word, count in top_words]


@app.get("/api/reviews", summary="按品类筛选评论", description="支持按品类筛选评论列表，可限制返回条数，为后续下钻交互做准备")
def get_reviews(cat: str = None, sentiment: str = None, query: str = None, indices: str = None, limit: int = 20):
    base = df
    if indices:
        try:
            idx_list = [int(i.strip()) for i in indices.split(",") if i.strip()]
            if idx_list:
                base = df.iloc[idx_list]
        except Exception:
            base = df
    filtered = filter_dataframe(cat=cat, sentiment=sentiment, base_df=base)
    if query:
        filtered = filtered[filtered["review"].notna()]
        try:
            filtered = filtered[
                filtered["review"].str.contains(query, case=False, regex=True)
            ]
        except Exception as error:
            print(f"⚠️ 正则表达式无效，已降级为普通字符串匹配: {query} | {error}")
            filtered = filtered[
                filtered["review"].str.contains(query, case=False, regex=False)
            ]
    records = filtered.head(limit).to_dict(orient="records")
    return {
        "total": len(filtered),
        "data": records
    }


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
