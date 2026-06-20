import os
import json
import asyncio
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI, APIError, RateLimitError, APIConnectionError
# 【任务3要求1】导入Tenacity相关模块
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==================== 1. 复用之前的配置（兼容lab10目录） ====================
load_dotenv("../lab09/lab09_api_key.env", override=True)
api_key = os.getenv("SILICONFLOW_API_KEY")
if not api_key:
    print("❌ 错误：无法读取lab09目录的API Key")
    exit(1)

client = AsyncOpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1",
    timeout=30
)

# ==================== 2. 复用实验9的Prompt模板 ====================
PROMPT_TEMPLATE = """
你是专业的电商数据流清洗组件，只负责从用户提供的商品评论中提取结构化特征，不进行任何其他对话。
请严格按照以下要求提取特征：
1. sentiment（情感倾向）：只能是 ["正面", "负面", "中性"] 中的一个，不能有其他值
2. category（问题归属）：只能是 ["物流", "质量", "价格", "服务", "综合"] 中的一个，不能有其他值
3. summary（核心诉求概括）：用不超过15个汉字概括评论的核心内容
输入评论：{review_text}
输出要求：
- 必须且仅能输出一个纯净的JSON对象
- 绝对不要包含任何解释性文字
- 绝对不要使用Markdown代码块标记
"""

# ==================== 3. 【任务3核心】带Tenacity指数退避重试的提取函数 ====================
# 【任务3要求2】挂载@retry装饰器，严格配置参数
@retry(
    # 最大重试10次（任务书要求）
    stop=stop_after_attempt(10),
    # 指数退避：初始2秒，每次翻倍，最大60秒（任务书要求）
    wait=wait_exponential(multiplier=1, min=2, max=60),
    # 【任务书要求】仅重试API相关异常，不掩盖语法/代码错误
    retry=retry_if_exception_type((RateLimitError, APIConnectionError, APIError)),
    # 打印重试日志，方便验证
    before_sleep=lambda s: print(f"⚠️  Tenacity触发重试：第{s.attempt_number}次，等待{s.next_action.sleep}秒后重试...")
)
async def extract_features(review_text: str, sem: asyncio.Semaphore) -> dict:
    final_prompt = PROMPT_TEMPLATE.format(review_text=review_text)
    
    # 保留任务2的Semaphore并发控制
    async with sem:
        response = await client.chat.completions.create(
            model="Qwen/Qwen3.5-4B",
            messages=[{"role": "user", "content": final_prompt}],
            max_tokens=200,
            temperature=0,
            extra_body={"enable_thinking": False}
        )
    
    json_str = response.choices[0].message.content.strip()
    
    try:
        result = json.loads(json_str)
        required_fields = ["sentiment", "category", "summary"]
        if all(field in result for field in required_fields):
            return result
        else:
            return {"sentiment": "错误", "category": "错误", "summary": "字段缺失"}
    except json.JSONDecodeError:
        return {"sentiment": "错误", "category": "错误", "summary": "格式错误"}

# ==================== 4. 主函数：正常运行+验证逻辑 ====================
async def main():
    print("=" * 80)
    print("📝 实验10 任务3：Tenacity指数退避重试机制")
    print("=" * 80)

    # ==================== 正常运行配置（验证完成后使用） ====================
    sem = asyncio.Semaphore(20)  # 正常运行用20并发
    # ==================== 验证重试机制配置（临时使用） ====================
    # sem = asyncio.Semaphore(100)  # 验证时临时取消注释，触发429限流

    print(f"✅ 当前并发数设置：{sem._value}")
    print("✅ Tenacity重试配置：最大10次，指数退避2s→60s")
    print("-" * 80)

    # 加载数据：验证用1000条，正常运行用100条
    df_raw = pd.read_csv("../lab09/online_shopping_10_cats.csv", encoding="utf-8")
    # 验证时用1000条，正常运行用100条
    test_reviews = df_raw['review'].iloc[0:100].tolist()  # 正常运行
    # test_reviews = df_raw['review'].iloc[0:1000].tolist()  # 验证时取消注释
    print(f"✅ 加载测试数据：共{len(test_reviews)}条评论")
    print("-" * 80)

    print("⏳ 开始并发处理...")
    start_time = asyncio.get_event_loop().time()
    
    tasks = [extract_features(review, sem) for review in test_reviews]
    results = await asyncio.gather(*tasks, return_exceptions=True)  # 捕获异常不中断
    
    total_time = asyncio.get_event_loop().time() - start_time

    # 统计结果
    success_count = sum(1 for r in results if isinstance(r, dict) and r['sentiment'] != '错误')
    error_count = len(results) - success_count

    print("-" * 80)
    print("📊 运行结果统计：")
    print(f"   总处理条数：{len(results)}")
    print(f"   成功条数：{success_count}")
    print(f"   错误/失败条数：{error_count}")
    print(f"   总耗时：{total_time:.2f} 秒")
    print("=" * 80)

    print("\n🎉 任务3完成：指数退避重试机制已生效！")
    print("💡 验证方法：取消代码中100并发+1000条数据的注释，运行后会看到Tenacity自动重试日志")

if __name__ == "__main__":
    asyncio.run(main())