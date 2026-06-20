"""
简易消费者观察脚本 (Observer)
持续监听 streaming_logs.jsonl，统计每 10 秒窗口内的点击数和购买数
"""

import json
import time
from pathlib import Path
from datetime import datetime

INPUT_FILE = "streaming_logs.jsonl"
WINDOW_SECONDS = 10


def tail_file(filepath: Path):
    """类似 tail -f，持续读取文件新增内容"""
    with open(filepath, "r", encoding="utf-8") as f:
        # 先定位到文件末尾
        f.seek(0, 2)
        
        while True:
            line = f.readline()
            if line:
                yield line
            else:
                time.sleep(0.1)  # 无新数据时短暂休眠


def main():
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        print(f"❌ 文件不存在: {input_path.absolute()}")
        print("请先运行 producer.py 生成数据")
        return
    
    print(f"👀 开始监听: {input_path.absolute()}")
    print(f"⏱️  统计窗口: {WINDOW_SECONDS} 秒")
    print(f"{'─' * 50}")
    print(f"{'时间':<20} | {'点击(view)':^10} | {'购买(purchase)':^10}")
    print(f"{'─' * 50}")
    
    window_start = time.time()
    view_count = 0
    purchase_count = 0
    
    for line in tail_file(input_path):
        try:
            event = json.loads(line.strip())
            behavior = event.get("behavior_type", "")
            
            if behavior == "view":
                view_count += 1
            elif behavior == "purchase":
                purchase_count += 1
            
            # 检查窗口是否结束
            now = time.time()
            if now - window_start >= WINDOW_SECONDS:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"{timestamp:<20} | {view_count:^12} | {purchase_count:^14}")
                
                # 重置窗口
                window_start = now
                view_count = 0
                purchase_count = 0
                
        except json.JSONDecodeError:
            continue
        except KeyboardInterrupt:
            print(f"\n\n🛑 停止监听")
            break


if __name__ == "__main__":
    main()
