# Метка: analyze_renko_files_v1
# Дата и время запуска: 2025-06-14 18:25:00

import os
import pandas as pd
import random
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

data_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
files = [f for f in os.listdir(data_dir) if f.endswith('.csv') and 'Renko' in f]

for file_name in files:
    file_path = os.path.join(data_dir, file_name)
    print(f"\nАнализ файла: {file_path}")

    # Чтение всего файла для получения случайных строк (с оптимизацией памяти)
    df = pd.read_csv(file_path, nrows=1000)  # Читаем первые 1000 строк для начальной выборки
    if len(df) < 1000:
        df_full = pd.read_csv(file_path)
    else:
        df_full = pd.read_csv(file_path, skiprows=lambda i: i > 0 and i <= 1000, nrows=len(df)-1000, header=None, names=df.columns)
        df = pd.concat([df, df_full])

    # Первые 5 строк
    print("Первые 5 строк (как в Блокноте):")
    with open(file_path, 'r', encoding='utf-8') as file:
        head_lines = [next(file) for _ in range(5) if file]
    for i, line in enumerate(head_lines, 1):
        print(f"Строка {i}: {line.strip()}")

    # Последние 5 строк
    with open(file_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    with open(file_path, 'r', encoding='utf-8') as file:
        if total_lines > 5:
            file.seek(0)
            last_lines = [next(file) for _ in range(total_lines - 5)]
            last_lines = [next(file) for _ in range(5)]
        else:
            file.seek(0)
            last_lines = [next(file) for _ in range(total_lines)]
    print("Последние 5 строк (как в Блокноте):")
    for i, line in enumerate(last_lines, 1):
        print(f"Строка {i}: {line.strip()}")

    # 5 случайных строк (исключая похожие на первые и последние)
    if len(df) > 10:  # Убедимся, что достаточно строк
        first_5 = [tuple(line.split(',')) for line in head_lines[1:]]  # Первые 4 данных строки
        last_5 = [tuple(line.split(',')) for line in last_lines]  # Последние 5 строк
        all_excluded = first_5 + last_5
        random_indices = random.sample(range(5, len(df) - 5), 5)  # Избегаем первых и последних 5 строк
        random_rows = df.iloc[random_indices].values
        print("5 случайных строк (как в Блокноте):")
        for i, row in enumerate(random_rows, 1):
            formatted_row = ','.join(str(x).strip() for x in row)
            print(f"Строка {i}: {formatted_row}")
    else:
        print("Недостаточно строк для случайного выбора.")

print(f"[{datetime.now()}] Анализ завершен")