# Метка: inspect_processed_files_20250601_1618
# Дата и время запуска: 01 июня 2025, 16:18 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

processed_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_processed"

for file in os.listdir(processed_dir):
    if file.endswith("_processed.csv"):
        file_path = os.path.join(processed_dir, file)

        # Размер файла
        file_size = os.path.getsize(file_path)

        print(f"\nФайл: {file}")
        print(f"Размер: {file_size} байт")

        # Первые 6 строк как в Блокноте
        print("Первые 6 строк (как в Блокноте):")
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                print(f"Строка {i}: {line.strip()}")
                if i == 6:
                    break

        # Чтение файла для анализа
        try:
            df = pd.read_csv(file_path, sep=",")

            # Выбор числовых столбцов
            numeric_cols = df.select_dtypes(include=['int64', 'float64', 'bool']).columns

            # Статистика для числовых столбцов
            print("\nОписательная статистика для числовых столбцов:")
            for col in numeric_cols:
                print(f"\nСтолбец: {col}")
                print(df[col].describe().to_string())

        except Exception as e:
            print(f"Ошибка при обработке {file}: {str(e)}")

print("\nПроверка завершена.")