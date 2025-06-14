# Метка: check_price_gaps_20250601_1300
# Дата и время запуска: 01 июня 2025, 13:00 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

input_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_processed"
price_columns = ["Open", "Close", "High", "Low"]

for file in os.listdir(input_dir):
    if file.endswith("_processed.csv") and ("Renko" in file or "Ticks" in file):
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path)

        # Проверка пропусков в ценах
        print(f"\nФайл: {file}")
        for col in price_columns:
            if col in df.columns:
                na_count = df[col].isna().sum()
                print(f"Пропуски в {col}: {na_count}")

        # Проверка временных интервалов (для Renko)
        if "Renko" in file and "Time (EET)" in df.columns:
            df["Time (EET)"] = pd.to_datetime(df["Time (EET)"])
            time_diffs = df["Time (EET)"].diff().dt.total_seconds().dropna()
            irregular_intervals = time_diffs[time_diffs > time_diffs.mean() * 2].count()
            print(f"Нерегулярные временные интервалы (>2x среднего): {irregular_intervals}")

        # Проверка отсутствия тиков (для Ticks)
        if "Ticks" in file and "Time (EET)" in df.columns:
            df["Time (EET)"] = pd.to_datetime(df["Time (EET)"])
            time_gaps = df["Time (EET)"].diff().dt.total_seconds().dropna()
            large_gaps = time_gaps[time_gaps > 60].count()  # Промежутки > 1 минуты
            print(f"Большие промежутки между тиками (>60 сек): {large_gaps}")

print("Проверка завершена.")