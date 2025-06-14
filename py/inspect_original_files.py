# Метка: inspect_original_files_20250601_1339
# Дата и время запуска: 01 июня 2025, 13:39 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"

for file in os.listdir(original_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(original_dir, file)

        # Размер файла
        file_size = os.path.getsize(file_path)

        # Чтение файла
        try:
            df = pd.read_csv(file_path, sep=";", nrows=6)

            # Столбцы
            columns = list(df.columns)

            # Первые 6 строк как в Блокноте
            with open(file_path, 'r', encoding='utf-8') as f:
                first_six_lines = [f.readline().strip() for _ in range(6)]

            # Вывод информации
            print(f"\nФайл: {file}")
            print(f"Размер: {file_size} байт")
            print(f"Столбцы: {columns}")
            print("Первые 6 строк (как в Блокноте):")
            for i, line in enumerate(first_six_lines, 1):
                print(f"Строка {i}: {line}")

        except Exception as e:
            print(f"Ошибка при обработке {file}: {str(e)}")

print("Проверка завершена.")