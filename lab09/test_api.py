import os
from dotenv import load_dotenv

# 直接指定现在的密钥文件名
load_dotenv("lab09_api_key.env", override=True)

# 读取API Key
api_key = os.getenv("SILICONFLOW_API_KEY")

# 验证
if api_key:
    print("✅ API Key读取成功！")
    print(f"密钥前10位：{api_key[:10]}...")
    print("\n✅ 完全符合实验安全要求：")
    print("1. 密钥没有写在.py文件中")
    print("2. 使用了python-dotenv库读取环境变量")
    print("3. 后续所有实验代码都可以复用这个配置")
else:
    print("❌ 读取失败！请检查：")
    print("1. lab09_api_key.env文件是否在lab09根目录下")
    print("2. 文件内是否写了 SILICONFLOW_API_KEY=你的密钥")
    print("3. 等号前后不要有空格，不要加引号")