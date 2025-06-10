# Метка: verify_original_processing_20250601_1252
# Дата и время запуска: 01 июня 2025, 12:52 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
other_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_other"
required_columns = ["Time (EET)", "Open", "High", "Low", "Close"]

# Проверка файлов в original
print("\nПроверка файлов в original:")
for file in os.listdir(original_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(original_dir, file)
        try:
            df = pd.read_csv(file_path, sep=";")
            print(f"\nФайл: {file}")
            print(f"Столбцы: {list(df.columns)}")
            print(f"Количество строк: {len(df)}")
            missing_cols = [col for col in required_columns if col not in df.columns]
            print(f"Отсутствующие столбцы: {missing_cols if missing_cols else 'Все присутствуют'}")
            if not missing_cols:
                print(f"Пропуски в ценах:\n{df[required_columns].isna().sum()}")
            try:
                pd.to_datetime(df["Time (EET)"])
                print("Формат Time: Корректный")
            except:
                print("Формат Time: Некорректный")
        except Exception as e:
            print(f"Ошибка при обработке {file}: {str(e)}")

# Проверка наличия промежуточных файлов
print("\nПроверка промежуточных файлов в data_reworked_other:")
for file in os.listdir(original_dir):
    if file.endswith(".csv"):
        base_name = file.replace(".csv", "")
        cleaned_file = f"{base_name}_cleaned.csv"
        merged_file = f"{base_name}_merged.csv"

        cleaned_path = os.path.join(other_dir, cleaned_file)
        merged_path = os.path.join(other_dir, merged_file)

        print(f"\nДля файла {file}:")
        print(f"Существует _cleaned.csv: {os.path.exists(cleaned_path)}")
        print(f"Существует _merged.csv: {os.path.exists(merged_path)}")

print("Проверка завершена.")