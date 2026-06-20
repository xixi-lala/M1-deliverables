import os
from dotenv import load_dotenv
from openai import OpenAI

# 1. 加载你的密钥文件（完全适配你的文件名）
load_dotenv("lab09_api_key.env", override=True)

# 2. 安全读取API Key
api_key = os.getenv("SILICONFLOW_API_KEY")
if not api_key:
    print("❌ 错误：无法读取API Key，请检查lab09_api_key.env文件")
    exit(1)

# 3. 初始化OpenAI客户端（硅基流动兼容OpenAI标准接口）
client = OpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1"  # 硅基流动的API地址
)

# 4. 发起测试请求
try:
    response = client.chat.completions.create(
        model="deepseek-ai/DeepSeek-V4-Flash",  # 实验指定的模型
        messages=[
            {"role": "user", "content": "你好，请回复测试成功。"}
        ],
        temperature=0  # 固定输出，减少随机性
    )

    # 5. 打印结果（实验要求截图完整的底层响应对象）
    print(response.choices[0].message.content)
    print("\n【完整底层响应对象】：")
    print(response)
    print("=" * 50)

except Exception as e:
    print(f"\n❌ API调用失败，错误信息：{str(e)}")
    print("\n常见错误排查：")
    print("1. 检查API Key是否复制正确，没有多/少字符")
    print("2. 检查网络是否正常，能否访问硅基流动官网")
    print("3. 检查账户余额是否充足（注册后有14元免费额度）")

    