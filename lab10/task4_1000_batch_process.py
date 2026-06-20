import os
import json
import asyncio
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI, APIError, RateLimitError, APIConnectionError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
# 【任务4要求3】导入异步进度条
from tqdm.asyncio import tqdm_asyncio

# ==================== 1. 复用配置（兼容lab10目录） ====================
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

# ==================== 2. 复用Prompt模板 ====================
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

# ==================== 3. 整合所有能力的提取函数 ====================
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((RateLimitError, APIConnectionError, APIError)),
    before_sleep=lambda s: print(f"\n⚠️  触发重试：第{s.attempt_number}次，等待{s.next_action.sleep}秒")
)
async def extract_features(review_text: str, sem: asyncio.Semaphore) -> dict:
    final_prompt = PROMPT_TEMPLATE.format(review_text=review_text)
    
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

# ==================== 4. 【任务4核心】1000条批量处理主控函数 ====================
async def main():
    print("=" * 100)
    print("📝 实验10 任务4：1000条数据高并发特征提取与落盘")
    print("=" * 100)

    # 1. 固定20并发（验证完成后的正式配置）
    sem = asyncio.Semaphore(20)
    print(f"✅ 并发控制：20并发上限")
    print(f"✅ 容错机制：Tenacity指数退避重试（最大10次）")
    print(f"✅ 进度监控：tqdm实时进度条")

    # 【任务4要求1】截取1000条评论
    df_raw = pd.read_csv("../lab09/online_shopping_10_cats.csv", encoding="utf-8")
    # df_1000 = df_raw.iloc[0:1000].reset_index(drop=True)  # 前1000条，重置索引保证对齐
    # 随机采样1000条，保证正负样本都存在
    df_1000 = df_raw.sample(n=1000, random_state=42).reset_index(drop=True)
    test_texts = df_1000['review'].tolist()
    print(f"✅ 加载数据集：成功截取前1000条评论")
    print("-" * 100)

    # 【任务4要求2】生成异步任务列表
    tasks = [extract_features(text, sem) for text in test_texts]

    # 【任务4要求3】带实时进度条的并发执行
    print("⏳ 开始并发特征提取...")
    start_time = asyncio.get_event_loop().time()
    
    # 用tqdm_asyncio.gather替代asyncio.gather，显示实时进度条
    results = await tqdm_asyncio.gather(*tasks, desc="特征提取进度", total=len(tasks))
    
    total_time = asyncio.get_event_loop().time() - start_time

    # 【任务4要求4】结果拼接与持久化
    print("\n" + "-" * 100)
    print("🔗 正在拼接原始数据与提取特征...")
    # 1. 把提取结果转成DataFrame
    df_features = pd.DataFrame(results)
    # 2. 水平拼接：原始数据 + 提取的3个特征列（索引对齐，保证一一对应）
    df_final = pd.concat([df_1000, df_features], axis=1)

    # 导出落盘，utf-8-sig避免Excel中文乱码
    OUTPUT_FILE = "batch_1000_features.csv"
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"💾 结果已成功落盘：{OUTPUT_FILE}")

    # ==================== 实验报告必填：耗时对比与加速倍数计算 ====================
    print("\n" + "=" * 100)
    print("📊 实验报告核心统计数据")
    print("=" * 100)
    sync_time_per = 8  # 实验9单线程平均耗时：8秒/条
    sync_total_time = len(test_texts) * sync_time_per  # 单线程理论总耗时
    speedup = sync_total_time / total_time  # 加速倍数

    print(f"📌 本次并发处理（20并发）：")
    print(f"   总数据量：{len(test_texts)} 条")
    print(f"   实际总耗时：{total_time:.2f} 秒 = {total_time/60:.2f} 分钟")
    print(f"   平均单条耗时：{total_time/len(test_texts):.2f} 秒")
    print(f"\n📌 实验9单线程（同步）理论耗时：")
    print(f"   理论总耗时：{sync_total_time:.2f} 秒 = {sync_total_time/60:.2f} 分钟")
    print(f"\n🚀 最终加速倍数：{speedup:.1f} 倍")

    # 数据质量校验
    success_count = sum(1 for r in results if r['sentiment'] != '错误')
    print(f"\n✅ 数据质量校验：")
    print(f"   提取成功：{success_count}/1000 条")
    print(f"   成功率：{success_count/10:.1f}%")
    print("=" * 100)
    print("\n🎉 任务4完成！1000条数据高并发特征提取全流程结束")
    print(f"📁 请打开 {OUTPUT_FILE} 查看最终结果，用于实验报告截图")

if __name__ == "__main__":
    asyncio.run(main())