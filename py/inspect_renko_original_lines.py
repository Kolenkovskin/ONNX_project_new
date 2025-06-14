# Метка: inspect_renko_original_lines_20250601_1640
# Дата и время запуска: 01 июня 2025, 16:40 EEST

import os
from datetime import datetime
import collections

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
renko_files = [
    "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv"
]

for file in renko_files:
    file_path = os.path.join(original_dir, file)
    if os.path.exists(file_path):
        print(f"\nФайл: {file}")

        # Первые 6 строк
        print("Первые 6 строк (как в Блокноте):")
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                print(f"Строка {i}: {line.strip()}")
                if i == 6:
                    break

        # Последние 5 строк
        print("\nПоследние 5 строк (как в Блокноте):")
        last_lines = collections.deque(maxlen=5)
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                last_lines.append(line.strip())

        for i, line in enumerate(last_lines, 1):
            print(f"Строка {i}: {line}")
    else:
        print(f"\nФайл {file} не найден.")

print("\nПроверка завершена.")