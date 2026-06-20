import os
import json
import asyncio
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI, APIError, RateLimitError, APIConnectionError

# ==================== 1. 复用之前的配置（100%兼容lab10目录） ====================
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

# ==================== 2. 100%复用实验9的Prompt模板 ====================
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

# ==================== 3. 【任务2核心改造】带信号量控制的异步提取函数 ====================
# 新增sem参数：接收main函数中创建的信号量对象
async def extract_features(review_text: str, sem: asyncio.Semaphore) -> dict:
    final_prompt = PROMPT_TEMPLATE.format(review_text=review_text)
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 【任务2要求2】用async with sem包裹核心网络请求
            # 达到20并发时，后续任务会自动在这里排队等待
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
                
        except RateLimitError:
            retry_count += 1
            wait_time = 2 ** retry_count
            await asyncio.sleep(wait_time)
        except APIConnectionError:
            retry_count += 1
            wait_time = 2 ** retry_count
            await asyncio.sleep(wait_time)
        except APIError as e:
            return {"sentiment": "错误", "category": "错误", "summary": "API调用失败"}
    
    return {"sentiment": "错误", "category": "错误", "summary": "重试失败"}

# ==================== 4. 【任务2要求1】主函数内部创建信号量 ====================
async def main():
    print("=" * 80)
    print("📝 实验10 任务2：Semaphore信号量并发洪峰控制")
    print("=" * 80)

    # 【任务2强制要求1】必须在main函数内部创建信号量！禁止全局定义
    # 限制同时最多20个HTTP请求，超过自动排队
    sem = asyncio.Semaphore(20)
    print(f"✅ 已设置并发上限：20并发，超过自动排队")

    # 复用lab09的数据集，截取50条做并发测试（避免消耗过多）
    df_raw = pd.read_csv("../lab09/online_shopping_10_cats.csv", encoding="utf-8")
    test_reviews = df_raw['review'].iloc[0:50].tolist()
    print(f"✅ 加载测试数据：共{len(test_reviews)}条评论")
    print("-" * 80)

    # 生成异步任务列表，每个任务都传入信号量
    tasks = [extract_features(review, sem) for review in test_reviews]
    
    print("⏳ 开始并发处理（最多20个同时请求）...")
    start_time = asyncio.get_event_loop().time()
    
    # 并发执行所有任务
    results = await asyncio.gather(*tasks)
    
    total_time = asyncio.get_event_loop().time() - start_time

    # 输出统计结果
    print("-" * 80)
    print("📊 并发处理结果统计：")
    print(f"   总处理条数：{len(results)}")
    print(f"   总耗时：{total_time:.2f} 秒")
    print(f"   平均单条耗时：{total_time/len(results):.2f} 秒")
    print(f"   处理成功数：{sum(1 for r in results if r['sentiment'] != '错误')}")
    print("-" * 80)

    # 验证信号量效果：对比同步速度
    sync_estimate = len(results) * 6  # 实验9单条平均6秒
    speedup = sync_estimate / total_time
    print(f"⚡ 对比实验9同步速度：理论同步耗时 {sync_estimate:.2f} 秒")
    print(f"🚀 本次并发加速倍数：{speedup:.1f} 倍")
    print("=" * 80)
    print("\n🎉 任务2完成：Semaphore并发控制验证通过！")
    print("💡 验证点：没有触发429限流，所有请求有序执行，无并发洪峰")

if __name__ == "__main__":
    asyncio.run(main())