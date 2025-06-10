# Метка: verify_cleaned_files_20250601_1449
# Дата и время запуска: 01 июня 2025, 14:49 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

cleaned_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_other"
expected_files = [
    "XAUUSD_1 Min_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_1 Min_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_30 Mins_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_30 Mins_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_Hourly_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_Hourly_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_Daily_Ask_2020.01.01_2025.05.25_cleaned.csv", "XAUUSD_Daily_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25_cleaned.csv",
    "XAUUSD_Ticks_2024.01.01_2025.05.25_cleaned.csv"
]

# Проверка наличия файлов
print("\nПроверка наличия файлов:")
missing_files = [f for f in expected_files if not os.path.exists(os.path.join(cleaned_dir, f))]
print(f"Отсутствующие файлы: {missing_files if missing_files else 'Все файлы присутствуют'}")

for file in os.listdir(cleaned_dir):
    if file.endswith("_cleaned.csv"):
        file_path = os.path.join(cleaned_dir, file)
        df = pd.read_csv(file_path, sep=",")

        print(f"\nФайл: {file}")
        print(f"Количество строк: {len(df)}")

        # Проверка столбцов
        columns = list(df.columns)
        print(f"Столбцы: {columns}")

        # Проверка типов данных
        print("Типы данных:")
        for col in columns:
            print(f"{col}: {df[col].dtype}")

        # Проверка пропусков
        print(f"Пропуски:\n{df.isna().sum()}")

        # Проверка логичности значений для Ticks
        if "Ticks" in file:
            if all(col in df.columns for col in ["Open", "High", "Low", "Close", "Ask", "Bid"]):
                invalid_high_low = (df["High"] < df["Low"]).sum()
                invalid_close = ((df["Close"] < df["Bid"]) | (df["Close"] > df["Ask"])).sum()
                print(f"Некорректные значения High < Low: {invalid_high_low}")
                print(f"Некорректные значения Close вне [Bid, Ask]: {invalid_close}")

print("Проверка завершена.")