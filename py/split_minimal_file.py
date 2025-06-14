# Метка: split_minimal_file_v1
# Дата и время запуска: 2025-06-13 22:02:00

import os
import pandas as pd
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp\XAUUSD_Ticks_2020.csv"
temp_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp"
os.makedirs(temp_dir, exist_ok=True)

# Удаление предыдущих частей, кроме если они не нужны
for file in os.listdir(temp_dir):
    if file.startswith("part_") and file.endswith(".csv"):
        os.remove(os.path.join(temp_dir, file))

# Разбиение на минимальные части
chunk_size = 10000
part_number = 0
first_part_path = None
last_part_path = None

for chunk in pd.read_csv(input_file, chunksize=chunk_size):
    part_file = os.path.join(temp_dir, f"part_{part_number:03d}.csv")
    chunk.to_csv(part_file, index=False)
    if part_number == 0:
        first_part_path = part_file
    last_part_path = part_file
    part_number += 1
    print(f"Создан файл: {part_file}")

# Удаление всех частей, кроме первого и последнего
for file in os.listdir(temp_dir):
    file_path = os.path.join(temp_dir, file)
    if file.startswith("part_") and file.endswith(".csv") and file_path not in [first_part_path, last_part_path]:
        os.remove(file_path)

print(f"Общее количество частей: {part_number}")
print(f"Сохранены файлы: {first_part_path}, {last_part_path}")

# Вывод первых 5 строк первого файла
if first_part_path and os.path.exists(first_part_path):
    print(f"Первые 5 строк {first_part_path} (как в Блокноте):")
    with open(first_part_path, 'r', encoding='utf-8') as file:
        head_lines = [next(file) for _ in range(5) if file]
    for i, line in enumerate(head_lines, 1):
        print(f"Строка {i}: {line.strip()}")

# Вывод последних 5 строк последнего файла
if last_part_path and os.path.exists(last_part_path):
    print(f"Последние 5 строк {last_part_path} (как в Блокноте):")
    with open(last_part_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    with open(last_part_path, 'r', encoding='utf-8') as file:
        if total_lines > 5:
            file.seek(0)
            last_lines = [next(file) for _ in range(total_lines - 5)]
            last_lines = [next(file) for _ in range(5)]
        else:
            file.seek(0)
            last_lines = [next(file) for _ in range(total_lines)]
    for i, line in enumerate(last_lines, 1):
        print(f"Строка {i}: {line.strip()}")

print(f"[{datetime.now()}] Скрипт завершён")