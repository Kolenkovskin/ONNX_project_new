# Метка: fill_indicator_gaps_renko_ticks_20250601_1538
# Дата и время запуска: 01 июня 2025, 15:38 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

input_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_processed"
indicators = ["RSI", "MACD"]  # Только RSI и MACD для Renko и Ticks

# Список файлов для обработки
target_files = [
    "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25_processed.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25_processed.csv",
    "XAUUSD_Ticks_2024.01.01_2025.05.25_processed.csv"
]

for file in os.listdir(input_dir):
    if file in target_files:
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path, sep=",")

        print(f"\nОбработка файла: {file}")
        initial_rows = len(df)

        # Обработка пропусков
        for indicator in indicators:
            if indicator in df.columns:
                df[indicator] = df[indicator].interpolate(method="linear", limit_direction="both")
                df[indicator] = df[indicator].bfill().ffill()  # Заполнение оставшихся пропусков

        # Проверка качества данных
        print(f"Количество строк после обработки: {len(df)}")
        print(f"Удалено строк: {initial_rows - len(df)}")
        print(f"Оставшиеся пропуски:\n{df[indicators].isna().sum()}")

        # Проверка логичности значений RSI
        if "RSI" in df.columns:
            invalid_rsi = ((df["RSI"] < 0) | (df["RSI"] > 100)).sum()
            print(f"Некорректные значения RSI (вне 0–100): {invalid_rsi}")

        # Сохранение обновленного файла
        df.to_csv(file_path, sep=",", index=False)
        print(f"Обновлен файл: {file_path}")

print("Обработка пропусков для Renko и Ticks завершена.")