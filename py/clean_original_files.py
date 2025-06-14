# Метка: clean_original_files_v2_20250601_1419
# Дата и время запуска: 01 июня 2025, 14:19 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_other"

# Создание папки, если не существует
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Создана папка: {output_dir}")

for file in os.listdir(original_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(original_dir, file)
        df = pd.read_csv(file_path, sep=";")

        # Удаление дубликатов
        initial_rows = len(df)
        df = df.drop_duplicates()
        print(f"\nФайл: {file}")
        print(f"Удалено дубликатов: {initial_rows - len(df)}")

        # Унификация столбцов
        if "Volume " in df.columns:
            df = df.rename(columns={"Volume ": "Volume"})
        if "OppositeWickPrice " in df.columns:
            df = df.rename(columns={"OppositeWickPrice ": "OppositeWickPrice"})

        # Преобразование числовых столбцов (замена запятых на точки)
        numeric_cols = [col for col in df.columns if col not in ["Time (EET)", "EndTime", "WickPrice"]]
        for col in numeric_cols:
            if df[col].dtype == object:
                try:
                    df[col] = df[col].str.replace(",", ".").astype(float)
                except Exception as e:
                    print(f"Ошибка преобразования столбца {col} в {file}: {str(e)}")

        # Для тиковых данных: создание Open, High, Low, Close
        if "Ticks" in file and "Ask" in df.columns and "Bid" in df.columns:
            df["Close"] = (df["Ask"] + df["Bid"]) / 2
            df["Open"] = df["Close"].shift(1).fillna(df["Close"])
            df["High"] = df[["Open", "Close"]].max(axis=1)
            df["Low"] = df[["Open", "Close"]].min(axis=1)

        # Сохранение очищенного файла
        output_path = os.path.join(output_dir, file.replace(".csv", "_cleaned.csv"))
        df.to_csv(output_path, sep=",", index=False)
        print(f"Создан файл: {output_path}")
        print(f"Количество строк: {len(df)}")

print("Очистка завершена.")