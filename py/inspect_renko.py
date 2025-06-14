# Метка: inspect_renko_v3
# Дата и время запуска: 2025-06-12 18:20:00

import pandas as pd
import os
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

base_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
years = range(2020, 2026)
timeframes = ["Ticks_Ask", "Ticks_Bid"]

for year in years:
    for timeframe in timeframes:
        file_path = os.path.join(base_dir, f"XAUUSD_Renko_ONE_PIP_{timeframe}_{year}.csv")
        if os.path.exists(file_path):
            print(f"Анализ файла: {file_path}")

            # Чтение первых 5 строк
            with open(file_path, 'r', encoding='utf-8') as file:
                head_lines = [next(file) for _ in range(5) if file]
            print("Первые 5 строк (как в Блокноте):")
            for i, line in enumerate(head_lines, 1):
                print(f"Строка {i}: {line.strip()}")

            # Чтение последних 10,000 строк для точного определения конца
            tail_lines = 10000
            with open(file_path, 'r', encoding='utf-8') as file:
                total_lines = sum(1 for _ in file)
            with open(file_path, 'r', encoding='utf-8') as file:
                if total_lines > tail_lines:
                    file.seek(0)
                    last_lines = [next(file) for _ in range(total_lines - tail_lines)]  # Пропускаем до нужной позиции
                    last_lines = [next(file) for _ in range(tail_lines)]  # Читаем последние 10,000
                else:
                    file.seek(0)
                    last_lines = [next(file) for _ in range(total_lines)]
            print("Последние 5 строк (как в Блокноте):")
            for i, line in enumerate(last_lines[-5:], 1):
                print(f"Строка {i}: {line.strip()}")
        else:
            print(f"Файл не найден: {file_path}")

print("Анализ завершен.")