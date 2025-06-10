# Метка: inspect_renko_by_year_v1
# Дата и время запуска: 2025-06-09 17:18:00

import pandas as pd
import os
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
years = range(2020, 2026)  # 2020–2025

files = [
            os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Ask_{year}.csv")
            for year in years
        ] + [
            os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Bid_{year}.csv")
            for year in years
        ]

for file_path in files:
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        continue

    print(f"\nАнализ файла: {file_path}")

    # Чтение первых 5 строк
    df_head = pd.read_csv(file_path, nrows=5)
    print("Первые 5 строк (как в Блокноте):")
    for idx, row in df_head.iterrows():
        row_str = ",".join(str(val) for val in row)
        print(f"Строка {idx + 1}: {row_str}")

    # Чтение последних 5 строк с оптимизацией
    print("\nПоследние 5 строк (как в Блокноте):")
    total_rows = sum(1 for _ in open(file_path)) - 1  # Минус заголовок
    skip_rows = max(0, total_rows - 5)
    df_tail = pd.read_csv(file_path, skiprows=skip_rows, nrows=5)
    for idx, row in enumerate(df_tail.itertuples(index=False), 1):
        row_str = ",".join(str(val) for val in row)
        print(f"Строка {idx}: {row_str}")

print("Анализ завершен.")