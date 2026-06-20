# 导入核心模块
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os

# 初始化FastAPI应用实例
app = FastAPI(
    title="大数据分析看板 API",
    version="1.0.0",
    description="Milestone4 前后端分离数据看板后端接口"
)

# ------------------------------
# 数据加载层（服务启动时执行一次，常驻内存）
# 已适配你的实际文件路径，无需修改
# ------------------------------
FEATURES_PATH = "../../lab10/batch_1000_features.csv"  # 实验10生成的LLM增强宽表
RAW_PATH = "../../lab09/online_shopping_10_cats.csv"   # 实验9的原始评论数据集

# 优先加载LLM增强数据，不存在则自动回退到原始数据集
if os.path.exists(FEATURES_PATH):
    df = pd.read_csv(FEATURES_PATH, encoding="utf-8-sig")
    print(f"✅ 已加载LLM增强数据: {len(df)} 条")
else:
    df = pd.read_csv(RAW_PATH, encoding="utf-8-sig")
    # 原始数据没有sentiment列，用label映射生成
    df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})
    print(f"⚠️  LLM增强数据不存在，已回退到原始数据: {len(df)} 条")

# ------------------------------
# 接口定义
# ------------------------------

# 1. 健康检查接口（原有保留）
@app.get("/api/health", summary="服务健康检查", description="验证后端服务是否正常运行")
def health_check():
    return {
        "status": "ok",
        "message": "大数据分析看板后端服务运行正常",
        "timestamp": "2026-06-02"
    }

# 2. 接口A：品类分布统计
@app.get("/api/category-distribution", summary="获取商品品类分布", description="返回各品类的样本数量，供前端柱状图/饼图使用")
def get_category_distribution():
    stats = df["cat"].value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist()
    }

# 3. 接口B：情感分析概览
@app.get("/api/sentiment-overview", summary="获取各品类情感分布", description="返回各品类下正面/负面/中性情感的数量，供前端堆叠柱状图使用")
def get_sentiment_overview():
    # 按品类和情感分组统计
    pivot = df.groupby(["cat", "sentiment"]).size().unstack(fill_value=0)
    # 转换为前端友好的格式
    result = []
    for cat in pivot.index:
        result.append({
            "category": cat,
            **{col: int(pivot.loc[cat, col]) for col in pivot.columns}
        })
    return {"data": result}

# 4. 接口C：按品类筛选评论（带查询参数）
@app.get("/api/reviews", summary="按品类筛选评论", description="支持按品类筛选评论列表，可限制返回条数，为后续下钻交互做准备")
def get_reviews(cat: str = None, limit: int = 20):
    """
    参数说明：
    - cat: 商品品类，可选，不填则返回所有品类
    - limit: 返回评论的最大条数，默认20条
    """
    # 按品类筛选
    filtered = df if cat is None else df[df["cat"] == cat]
    # 限制返回条数并转换为字典列表
    records = filtered.head(limit).to_dict(orient="records")
    return {
        "total": len(filtered),  # 符合条件的总条数
        "data": records          # 评论数据列表
    }

# ------------------------------
# 跨域配置与静态文件服务（必须写在所有路由的最后面）
# ------------------------------

# 配置CORS跨域中间件，开发环境允许所有来源
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 将frontend文件夹挂载为根路径的静态文件服务
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")