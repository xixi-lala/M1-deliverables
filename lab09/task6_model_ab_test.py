import os
import json
import time
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI, APIError, RateLimitError, APIConnectionError

# ==================== 1. 基础配置 ====================
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

# ==================== 2. 可配置模型的特征提取函数（核心：一键切换） ====================
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

def extract_features(review_text: str, model_id: str) -> dict:
    """
    可指定模型ID的特征提取函数（实现模型解耦）
    :param review_text: 原始评论文本
    :param model_id: 要调用的模型ID
    :return: 结构化特征字典
    """
    final_prompt = PROMPT_TEMPLATE.format(review_text=review_text)
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = client.chat.completions.create(
                model=model_id,  # 唯一需要修改的地方：模型ID
                messages=[{"role": "user", "content": final_prompt}],
                response_format={"type": "json_object"},
                temperature=0,
                max_tokens=100
            )
            
            json_str = response.choices[0].message.content.strip()
            
            try:
                result = json.loads(json_str)
                required_fields = ["sentiment", "category", "summary"]
                if all(field in result for field in required_fields):
                    # 字段校验
                    if result["sentiment"] not in ["正面", "负面", "中性"]:
                        result["sentiment"] = "格式错误"
                    if result["category"] not in ["物流", "质量", "价格", "服务", "综合"]:
                        result["category"] = "格式错误"
                    if len(result["summary"]) > 15:
                        result["summary"] = result["summary"][:15]
                    result["format_valid"] = True
                    return result
                else:
                    return {"sentiment": "格式错误", "category": "格式错误", "summary": "字段缺失", "format_valid": False}
                    
            except json.JSONDecodeError:
                return {"sentiment": "格式错误", "category": "格式错误", "summary": "JSON解析失败", "format_valid": False}
                
        except RateLimitError:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"⚠️  {model_id} 触发限流，等待{wait_time}秒后重试...（第{retry_count}次）")
            time.sleep(wait_time)
        except APIConnectionError:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"⚠️  {model_id} 网络连接失败，等待{wait_time}秒后重试...（第{retry_count}次）")
            time.sleep(wait_time)
        except APIError as e:
            print(f"❌ {model_id} API错误：{str(e)}")
            return {"sentiment": "API错误", "category": "API错误", "summary": "调用失败", "format_valid": False}
    
    return {"sentiment": "调用失败", "category": "调用失败", "summary": "重试次数用完", "format_valid": False}

# ==================== 3. 批量测试函数 ====================
def batch_test_model(model_id: str, test_reviews: list) -> tuple:
    """
    批量测试指定模型，返回结果列表和总耗时
    """
    print(f"\n🚀 开始测试模型：{model_id}")
    start_time = time.perf_counter()
    results = []
    
    for i, review in enumerate(test_reviews):
        print(f"   处理第{i+1}条评论...")
        feature = extract_features(review, model_id)
        results.append(feature)
    
    total_time = time.perf_counter() - start_time
    print(f"✅ {model_id} 测试完成，总耗时：{total_time:.2f} 秒")
    
    return results, total_time

# ==================== 4. 主函数：A/B测试 ====================
if __name__ == "__main__":
    print("=" * 100)
    print("🔬 任务六：基座模型热切换与A/B测试")
    print("=" * 100)
    
    DATASET_PATH = "D:\MyProjects\DataAnalysis\lab09\online_shopping_10_cats.csv"
    # 加载与任务四完全相同的5条测试评论（保证对比公平性）
    try:
        df_raw = pd.read_csv(DATASET_PATH, encoding="utf-8")
        test_reviews = df_raw['review'].iloc[100:105].tolist()
        print(f"✅ 加载测试数据：第100-104行共5条评论")
    except FileNotFoundError:
        print("❌ 错误：找不到数据集文件")
        exit(1)
    
    # 定义测试模型
    MODELS = {
        "DeepSeek-V4-Flash": "deepseek-ai/DeepSeek-V4-Flash",
        "Qwen3.5-4B": "Qwen/Qwen3.5-4B"
    }
    
    # 执行A/B测试
    model_results = {}
    for model_name, model_id in MODELS.items():
        results, total_time = batch_test_model(model_id, test_reviews)
        model_results[model_name] = {
            "results": results,
            "total_time": total_time,
            "avg_time": total_time / 5
        }
    
    # ==================== 5. 生成对比表格（实验报告核心） ====================
    print("\n" + "=" * 100)
    print("📊 模型A/B测试对比结果")
    print("=" * 100)
    
    # 构建对比DataFrame
    comparison_data = []
    for i in range(5):
        row = {
            "序号": i+1,
            "原始评论": test_reviews[i][:40] + "..." if len(test_reviews[i]) > 40 else test_reviews[i],
            "DeepSeek_情感": model_results["DeepSeek-V4-Flash"]["results"][i]["sentiment"],
            "DeepSeek_分类": model_results["DeepSeek-V4-Flash"]["results"][i]["category"],
            "DeepSeek_摘要": model_results["DeepSeek-V4-Flash"]["results"][i]["summary"],
            "Qwen3.5_情感": model_results["Qwen3.5-4B"]["results"][i]["sentiment"],
            "Qwen3.5_分类": model_results["Qwen3.5-4B"]["results"][i]["category"],
            "Qwen3.5_摘要": model_results["Qwen3.5-4B"]["results"][i]["summary"],
            "情感分歧": "❌" if model_results["DeepSeek-V4-Flash"]["results"][i]["sentiment"] != model_results["Qwen3.5-4B"]["results"][i]["sentiment"] else "✅",
            "分类分歧": "❌" if model_results["DeepSeek-V4-Flash"]["results"][i]["category"] != model_results["Qwen3.5-4B"]["results"][i]["category"] else "✅",
            "摘要分歧": "❌" if model_results["DeepSeek-V4-Flash"]["results"][i]["summary"] != model_results["Qwen3.5-4B"]["results"][i]["summary"] else "✅"
        }
        comparison_data.append(row)
    
    df_comparison = pd.DataFrame(comparison_data)
    
    # 打印对比表格
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1200)
    pd.set_option('display.max_colwidth', 40)
    print(df_comparison)
    
    # ==================== 6. 统计分析（实验报告需要） ====================
    print("\n" + "=" * 100)
    print("📈 整体统计分析")
    print("=" * 100)
    
    # 计算格式服从度
    deepseek_format_valid = sum(1 for r in model_results["DeepSeek-V4-Flash"]["results"] if r["format_valid"])
    qwen_format_valid = sum(1 for r in model_results["Qwen3.5-4B"]["results"] if r["format_valid"])
    
    # 计算特征对齐度
    sentiment_agreement = sum(1 for i in range(5) if model_results["DeepSeek-V4-Flash"]["results"][i]["sentiment"] == model_results["Qwen3.5-4B"]["results"][i]["sentiment"])
    category_agreement = sum(1 for i in range(5) if model_results["DeepSeek-V4-Flash"]["results"][i]["category"] == model_results["Qwen3.5-4B"]["results"][i]["category"])
    
    # 生成统计表格
    stats_data = [
        ["总耗时（秒）", f"{model_results['DeepSeek-V4-Flash']['total_time']:.2f}", f"{model_results['Qwen3.5-4B']['total_time']:.2f}"],
        ["平均单条耗时（秒）", f"{model_results['DeepSeek-V4-Flash']['avg_time']:.2f}", f"{model_results['Qwen3.5-4B']['avg_time']:.2f}"],
        ["格式正确数/5", f"{deepseek_format_valid}/5", f"{qwen_format_valid}/5"],
        ["格式服从度", f"{deepseek_format_valid/5*100:.0f}%", f"{qwen_format_valid/5*100:.0f}%"],
        ["情感对齐数/5", f"{sentiment_agreement}/5", "-"],
        ["分类对齐数/5", f"{category_agreement}/5", "-"],
        ["特征对齐度", f"{(sentiment_agreement+category_agreement)/10*100:.0f}%", "-"]
    ]
    
    df_stats = pd.DataFrame(stats_data, columns=["指标", "DeepSeek-V4-Flash", "Qwen3.5-4B"])
    print(df_stats.to_string(index=False))
    
    # 保存结果
    df_comparison.to_csv("model_ab_test_comparison.csv", index=False, encoding="utf-8-sig")
    df_stats.to_csv("model_ab_test_stats.csv", index=False, encoding="utf-8-sig")
    print("\n💾 对比结果已保存为 model_ab_test_comparison.csv 和 model_ab_test_stats.csv")
    
  