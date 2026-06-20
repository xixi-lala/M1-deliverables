# -*- coding: utf-8 -*-
"""
文件名称：read_csv_polars.py
功能描述：使用 Polars 库读取 CSV 文件并打印前 10 行数据
作者：User
创建日期：2026-03-16
"""

import polars as pl


def main():
    """
    主函数：读取 CSV 文件并打印前 10 行
    """
    # 指定 CSV 文件路径（请根据实际情况修改文件路径）
    csv_file_path = "data.csv"
    
    # 使用 Polars 读取 CSV 文件
    # read_csv 会自动推断列的数据类型
    df = pl.read_csv(csv_file_path)
    
    # 打印数据集的基本信息
    print("=" * 50)
    print(f"CSV 文件：{csv_file_path}")
    print(f"数据形状：{df.shape[0]} 行 × {df.shape[1]} 列")
    print("=" * 50)
    
    # 打印前 10 行数据
    print("\n前 10 行数据：")
    print(df.head(10))


if __name__ == "__main__":
    main()
