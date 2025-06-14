# Метка: inspect_all_original_lines_20250601_1643
# Дата и время запуска: 01 июня 2025, 16:43 EEST

import os
from datetime import datetime
import collections

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"

for file in os.listdir(original_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(original_dir, file)
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

print("\nПроверка завершена.")