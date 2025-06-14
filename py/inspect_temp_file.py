# Метка: inspect_temp_file_v1
# Дата и время запуска: 2025-06-13 21:58:00

import os
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

file_path = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp\XAUUSD_Ticks_2020.csv"

if os.path.exists(file_path):
    print(f"Анализ файла: {file_path}")

    # Чтение первых 5 строк
    with open(file_path, 'r', encoding='utf-8') as file:
        head_lines = [next(file) for _ in range(5) if file]
    print("Первые 5 строк (как в Блокноте):")
    for i, line in enumerate(head_lines, 1):
        print(f"Строка {i}: {line.strip()}")

    # Чтение последних 5 строк
    with open(file_path, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    with open(file_path, 'r', encoding='utf-8') as file:
        if total_lines > 5:
            file.seek(0)
            last_lines = [next(file) for _ in range(total_lines - 5)]  # Пропускаем до последних 5
            last_lines = [next(file) for _ in range(5)]  # Читаем последние 5
        else:
            file.seek(0)
            last_lines = [next(file) for _ in range(total_lines)]
    print("Последние 5 строк (как в Блокноте):")
    for i, line in enumerate(last_lines, 1):
        print(f"Строка {i}: {line.strip()}")
else:
    print(f"Файл не найден: {file_path}")

print(f"[{datetime.now()}] Анализ завершен")