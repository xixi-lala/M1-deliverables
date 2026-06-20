import os
import json
import time
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
    # 从online_shopping_10_cats.csv中手动复制的真实复杂评论
    # 你可以替换成其他评论进行测试
    test_reviews = [
        "这是我第一次通过当当网买书，朋友说这里买很合算，于是试试看。通过这次买书,觉得当当送书的速度很快，隔天就到了。书和以前在书店里买的《走遍德国》比对了一下，发现就是少了外研社的标志，我不知道是不是正版的。翻看内容，印刷质量还蛮好的，应该没什么问题。很快就要用这本教材了，如果没问题，我以后还会选择当当网买书。整体感觉还是挺不错的",
        "1分给京东，促销的套路耍得好，端午活动价*31日下午收到货，检查好，开始充电，还没开玩，6月1号凌晨发短信，你关注的商品特价*先让你这批持续关注的用户降一点就买了，隔一天再降一次让那批犹豫不决的也赶快买，占便宜啦，快买！申请价保无效，提示“活动降价，无法价保”，端午节做的就不是活动？",
        "D3100缺少DIY功能（没有软件支持，不能直接对手机的电话本、铃声、待机图案等进行操作、设置，所以缺少了很多的乐趣），不支持MMS及EMS,只支持SMS。机身不能进行短消息存储，不支持关机闹钟。设计原因导致显示屏部分地方容易磨损，读取sim卡时间过长导致读取短信速度很慢。还有一个不太起眼的地方就是在安装SIM卡的时候，SIM卡锁很难打开和闭合，或者可以说是需要很大的力气和技巧。令人颇为之费劲。",
        "买了好几次了，这一次最不满意、10个烂了4个，可能会说是运输的问题，同样的包装之前就不会、而且烂的位置明显不是碰撞到的，我损失的是几个苹果，商家损失的是我再也不买你们家的！给你们32个赞",
        "屏幕虽然出色，但是色系偏暖色调，可能有人会不习惯，USB的设计不太合理都在一面且是并列的，这个缺点sony从sr1以来一直没改，比较失望。光驱处空隙较大。"
    ]
    
    print("=" * 70)
    print("📝 任务三：结构化特征抽取单条验证")
    print("=" * 70)
    
    for i, review in enumerate(test_reviews[:1]):  # 先只测试第一条，避免消耗过多额度
        print(f"\n🔍 原始评论：{review}")
        print("-" * 70)
        
        # 调用特征提取函数
        result = extract_features(review)
        
        # 打印结果（实验报告需要截图）
        print("✅ 提取结果：")
        print(f"  情感倾向：{result['sentiment']}")
        print(f"  问题归属：{result['category']}")
        print(f"  核心诉求：{result['summary']}")
        print("-" * 70)
        print("📦 完整JSON字典：")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("=" * 70)