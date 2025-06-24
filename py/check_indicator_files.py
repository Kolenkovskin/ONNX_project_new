# Метка: check_indicator_files_v1
# Дата и время запуска: 2025-06-23 17:26:00
# Ожидаемое время выполнения: ~600 секунд (~10 минут)
# Версия: check_indicator_files.py v1

import pandas as pd
import os
import random
from datetime import datetime
import time

print(f"[{datetime.now()}] Скрипт check_indicator_files_v1 запущен")


def print_file_samples(file_path):
    """Вывод 5 первых, 5 последних и 5 случайных строк файла."""
    print(f"\n[{datetime.now()}] Обработка файла: {os.path.basename(file_path)}")
    try:
        # Чтение файла
        df = pd.read_csv(file_path, delimiter=';', encoding='utf-8-sig')

        # 5 первых строк
        print(f"[{datetime.now()}] 5 первых строк:")
        print(df.head(5).to_string(index=False))

        # 5 последних строк
        print(f"[{datetime.now()}] 5 последних строк:")
        print(df.tail(5).to_string(index=False))

        # 5 случайных строк (исключая первые и последние 5)
        if len(df) > 10:
            random_indices = random.sample(range(5, len(df) - 5), 5)
            print(f"[{datetime.now()}] 5 случайных строк:")
            print(df.iloc[random_indices].to_string(index=False))
        else:
            print(f"[{datetime.now()}] Недостаточно строк для случайного выбора")

    except Exception as e:
        print(f"[{datetime.now()}] Ошибка обработки {file_path}: {e}")


# Основной код
start_time = time.time()
try:
    input_dir = r"D:\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"
    for file in os.listdir(input_dir):
        if file.endswith('_indicators.csv'):
            file_path = os.path.join(input_dir, file)
            print_file_samples(file_path)

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"[{datetime.now()}] Проверка завершена")
print(f"[{datetime.now()}] Генерация проверки завершена. Общее время: {time.time() - start_time:.2f} секунд")