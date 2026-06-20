import os
import json
import time
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI, APIError, RateLimitError, APIConnectionError

# ==================== 1. 基础配置（复用之前的环境） ====================
load_dotenv("lab09_api_key.env", override=True)
api_key = os.getenv("SILICONFLOW_API_KEY")

if not api_key:
    print("❌ 错误：无法读取API Key")
    exit(1)

client = OpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1",
    timeout=30
)

# ==================== 2. 实验要求的Prompt模板（核心！实验报告需要提交） ====================
# 完整Prompt字符串，直接复制到实验报告中
PROMPT_TEMPLATE = """
你是专业的电商数据流清洗组件，只负责从用户提供的商品评论中提取结构化特征，不进行任何其他对话。

请严格按照以下要求提取特征：
1. sentiment（情感倾向）：只能是 ["正面", "负面", "中性"] 中的一个，不能有其他值
2. category（问题归属）：只能是 ["物流", "质量", "价格", "服务", "综合"] 中的一个，不能有其他值
3. summary（核心诉求概括）：用不超过15个汉字概括评论的核心内容

输入评论：{review_text}

输出要求：
- 必须且仅能输出一个纯净的JSON对象
- 绝对不要包含任何解释性文字，如"好的"、"这是你的JSON"等
- 绝对不要使用Markdown代码块标记
- 严格遵守字段名和取值范围
"""

# ==================== 3. 带重试和异常处理的特征提取函数 ====================
def extract_features(review_text: str) -> dict:
    """
    从单条商品评论中提取结构化特征
    :param review_text: 原始评论文本
    :return: 包含sentiment, category, summary的字典，解析失败返回错误标记
    """
    # 构造最终Prompt
    final_prompt = PROMPT_TEMPLATE.format(review_text=review_text)
    
    # 指数退避重试
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = client.chat.completions.create(
                model="deepseek-ai/DeepSeek-V4-Flash",
                messages=[{"role": "user", "content": final_prompt}],
                response_format={"type": "json_object"},  # API层强制JSON输出
                temperature=0,  # 固定输出，减少随机性
                max_tokens=100  # 限制输出长度，防止生成多余内容
            )
            
            # 获取返回的JSON字符串
            json_str = response.choices[0].message.content.strip()
            
            # 尝试解析JSON
            try:
                result = json.loads(json_str)
                # 验证是否包含所有必需字段
                required_fields = ["sentiment", "category", "summary"]
                if all(field in result for field in required_fields):
                    return result
                else:
                    print(f"⚠️ 模型返回缺少字段：{json_str}")
                    return {"sentiment": "错误", "category": "错误", "summary": "字段缺失"}
                    
            except json.JSONDecodeError:
                print(f"⚠️ JSON解析失败，原始输出：{json_str}")
                return {"sentiment": "错误", "category": "错误", "summary": "格式错误"}
                
        except RateLimitError:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"⚠️ 触发限流，等待{wait_time}秒后重试...（第{retry_count}次）")
            time.sleep(wait_time)
        except APIConnectionError:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"⚠️ 网络连接失败，等待{wait_time}秒后重试...（第{retry_count}次）")
            time.sleep(wait_time)
        except APIError as e:
            print(f"❌ API错误：{str(e)}")
            return {"sentiment": "错误", "category": "错误", "summary": "API调用失败"}
    
    print("❌ 重试次数已用完")
    return {"sentiment": "错误", "category": "错误", "summary": "调用失败"}

# ==================== 4. 单条解析闭环验证（实验要求） ====================
if __name__ == "__main__":
    DATASET_PATH = "D:\MyProjects\DataAnalysis\lab09\online_shopping_10_cats.csv"
    
    print("=" * 80)
    print("📊 任务四：小批量串行处理与DataFrame重构")
    print("=" * 80)
    
    # 步骤1：读取数据集并截取5条测试数据（严格按照实验要求用iloc[100:105]）
    try:
        df_raw = pd.read_csv(DATASET_PATH, encoding="utf-8")
        # 截取第100到104行（共5条），获取review列
        test_reviews = df_raw['review'].iloc[100:105].tolist()
        # 同时保存原始数据的其他列，用于后续任务五的拼接
        test_raw_data = df_raw.iloc[100:105].reset_index(drop=True)
        print(f"✅ 成功读取数据集，截取第100-104行共5条评论")
    except FileNotFoundError:
        print(f"❌ 错误：找不到数据集文件 {DATASET_PATH}")
        print("请确保将online_shopping_10_cats.csv放在lab09/data/目录下")
        exit(1)
    
    # 步骤2：批量处理并记录总耗时
    print("\n⏳ 开始批量处理5条评论...")
    start_time = time.perf_counter()  # 使用高精度计时器
    results = []
    
    for i, review in enumerate(test_reviews):
        print(f"\n🔍 处理第{i+1}条评论：{review[:30]}...")
        feature = extract_features(review)
        results.append(feature)
        print(f"✅ 处理完成：{feature}")
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    # 步骤3：转换为结构化DataFrame
    df_features = pd.DataFrame(results)
    
    # 步骤4：输出结果（实验报告需要截图）
    print("\n" + "=" * 80)
    print("📋 批量处理结果（结构化DataFrame）：")
    print("=" * 80)
    print(df_features)
    print("\n" + "=" * 80)
    print(f"⏱️  串行处理5条数据总耗时：{total_time:.2f} 秒")
    print(f"⚡ 平均单条耗时：{total_time/5:.2f} 秒")
    print("=" * 80)
    
    # 保存中间结果，方便任务五使用
    df_features.to_csv("temp_features.csv", index=False, encoding="utf-8-sig")
    test_raw_data.to_csv("temp_raw_data.csv", index=False, encoding="utf-8-sig")
    print("\n💾 中间结果已保存为temp_features.csv和temp_raw_data.csv")