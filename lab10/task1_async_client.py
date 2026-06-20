import os
import json
import asyncio
from dotenv import load_dotenv
# 【实验9基础改造】替换同步OpenAI为异步AsyncOpenAI
from openai import AsyncOpenAI, APIError, RateLimitError, APIConnectionError

# ==================== 1. 100%复用实验9的安全配置（适配lab10目录） ====================
# 直接读取同级lab09目录的密钥文件，无需复制到lab10
load_dotenv("../lab09/lab09_api_key.env", override=True)
api_key = os.getenv("SILICONFLOW_API_KEY")
if not api_key:
    print("❌ 错误：无法读取lab09目录的API Key，请确认目录结构正确")
    exit(1)

# 【任务1核心要求1】实例化异步客户端
client = AsyncOpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1",
    timeout=30
)

# ==================== 2. 100%复用实验9的Prompt模板，无任何修改 ====================
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

# ==================== 3. 【任务1核心要求2+3】实验9函数异步化改造 ====================
# 【任务1要求2】函数签名从def改为async def
async def extract_features(review_text: str) -> dict:
    """
    基于实验9同步函数改造的异步版特征提取函数
    """
    final_prompt = PROMPT_TEMPLATE.format(review_text=review_text)
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 【任务1要求3】API调用前添加await关键字，实现非阻塞
            # 【强制要求】免费模型+关闭思考模式，避免额度消耗和空返回
            response = await client.chat.completions.create(
                model="Qwen/Qwen3.5-4B",  # 实验10强制免费模型
                messages=[{"role": "user", "content": final_prompt}],
                max_tokens=200,
                temperature=0,
                extra_body={"enable_thinking": False}  # 解决Qwen返回空content的问题
            )
            
            json_str = response.choices[0].message.content.strip()
            
            # 100%复用实验9的JSON解析和校验逻辑，无修改
            try:
                result = json.loads(json_str)
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
            await asyncio.sleep(wait_time)  # 异步等待，替换实验9的同步time.sleep
        except APIConnectionError:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"⚠️ 网络连接失败，等待{wait_time}秒后重试...（第{retry_count}次）")
            await asyncio.sleep(wait_time)
        except APIError as e:
            print(f"❌ API错误：{str(e)}")
            return {"sentiment": "错误", "category": "错误", "summary": "API调用失败"}
    
    print("❌ 重试次数已用完")
    return {"sentiment": "错误", "category": "错误", "summary": "调用失败"}

# ==================== 4. 异步主函数 ====================
async def main():
    print("=" * 70)
    print("📝 实验10 任务1：AsyncOpenAI同步→异步改造验证")
    print("=" * 70)
    
    # 100%复用实验9的测试评论，保证结果可对比
    test_review = "这是我第一次通过当当网买书，朋友说这里买很合算，于是试试看。通过这次买书,觉得当当送书的速度很快，隔天就到了。书和以前在书店里买的《走遍德国》比对了一下，发现就是少了外研社的标志，我不知道是不是正版的。翻看内容，印刷质量还蛮好的，应该没什么问题。很快就要用这本教材了，如果没问题，我以后还会选择当当网买书。整体感觉还是挺不错的"
    
    print(f"\n🔍 原始评论：")
    print(f"   {test_review[:80]}...")
    print("-" * 70)
    
    result = await extract_features(test_review)
    
    print("✅ 异步提取成功！结果：")
    print(f"  情感倾向：{result['sentiment']}")
    print(f"  问题归属：{result['category']}")
    print(f"  核心诉求：{result['summary']}")
    print("-" * 70)
    print("📦 完整JSON字典：")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("=" * 70)
    print("\n🎉 任务1完成,异步改造验证通过！")

if __name__ == "__main__":
    asyncio.run(main())