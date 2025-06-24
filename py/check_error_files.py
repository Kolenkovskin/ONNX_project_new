# check_error_files_detailed.py
# Version: 1.0
# Launch Date: 2025-06-23 19:51:00
# Expected Execution Time: ~150 seconds

import pandas as pd
import os
import sys
import time
import random
import psutil

print(f"[2025-06-23 19:51:00] Скрипт check_error_files_detailed_v1 запущен")
start_time = time.time()

# Проверка доступной памяти
memory = psutil.virtual_memory()
print(f"[2025-06-23 19:51:00] Доступная память: {memory.available / (1024 ** 3):.2f} GB")

# Пути к файлам с ошибками
error_files = [
    r"D:\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020_2025_indicators.csv",
    r"D:\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators\XAUUSD_Ticks_2020.01.01_2025.05.25_cleaned_indicators.csv"
]

for file_path in error_files:
    if not os.path.exists(file_path):
        print(f"[2025-06-23 19:51:00] Файл не найден: {file_path}")
        continue

    print(f"[2025-06-23 19:51:00] Обработка файла: {file_path}")
    try:
        # Чтение первых 5, последних 5 и 5 случайных строк
        df = pd.read_csv(file_path, nrows=10)  # Для получения первых 10 строк
        first_5 = df.head(5).to_string(index=False).replace('\n', '\n')
        last_5 = pd.read_csv(file_path, skiprows=range(1, max(0, len(df) - 5))).head(5).to_string(index=False).replace(
            '\n', '\n')

        # Чтение для случайных строк (избегаем первых и последних 5)
        total_rows = sum(1 for _ in open(file_path)) - 10  # Оценка общего числа строк
        if total_rows > 10:
            random_rows = random.sample(range(5, total_rows - 5), 5)
            random_5 = pd.read_csv(file_path, skiprows=lambda i: i not in random_rows, nrows=5).to_string(
                index=False).replace('\n', '\n')
        else:
            random_5 = "Недостаточно строк для случайного выбора"

        print(f"[2025-06-23 19:51:00] 5 первых строк (как в Блокноте):")
        print(first_5)
        print(f"[2025-06-23 19:51:00] 5 последних строк (как в Блокноте):")
        print(last_5)
        print(f"[2025-06-23 19:51:00] 5 случайных строк (как в Блокноте, не первые/последние):")
        print(random_5)

    except MemoryError as e:
        print(f"[2025-06-23 19:51:00] Ошибка обработки {file_path}: {str(e)}")
    except Exception as e:
        print(f"[2025-06-23 19:51:00] Ошибка обработки {file_path}: {str(e)}")

end_time = time.time()
total_time = end_time - start_time
print(f"[2025-06-23 19:51:00] Проверка завершена. Общее время: {total_time:.2f} секунд")
print(f"[2025-06-23 19:51:00] Генерация проверки завершена.")

# Выполнить в PyCharm: File -> Open -> D:\PycharmProjects\ONNX_bot\py -> Создать файл check_error_files_detailed.py -> Вставить код -> Run