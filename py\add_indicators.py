# Метка: add_indicators_20250601_1458
# Дата и время запуска: 01 июня 2025, 14:58 EEST

import pandas as pd
import os
from datetime import datetime
import ta

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

input_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_other"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_processed"

# Создание папки, если не существует
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Создана папка: {output_dir}")

for file in os.listdir(input_dir):
    if file.endswith("_cleaned.csv"):
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path, sep=",")

        print(f"\nОбработка файла: {file}")

        # Добавление индикаторов
        if "Close" in df.columns:
            if "Ticks" not in file:
                # RSI (период 14)
                df["RSI"] = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
                # MACD (12, 26, 9)
                df["MACD"] = ta.trend.MACD(df["Close"], window_slow=26, window_fast=12, window_sign=9).macd()
                # Bollinger Bands (период 20)
                df["BB_High"] = ta.volatility.BollingerBands(df["Close"], window=20, window_dev=2).bollinger_hband()
                df["BB_Low"] = ta.volatility.BollingerBands(df["Close"], window=20, window_dev=2).bollinger_lband()
                # ATR (период 14)
                df["ATR"] = ta.volatility.AverageTrueRange(df["High"], df["Low"], df["Close"],
                                                           window=14).average_true_range()
                # Stochastic (период 14, K=3, D=3)
                df["Stochastic_K"] = ta.momentum.StochasticOscillator(df["High"], df["Low"], df["Close"], window=14,
                                                                      smooth_window=3).stoch()
                df["Stochastic_D"] = ta.momentum.StochasticOscillator(df["High"], df["Low"], df["Close"], window=14,
                                                                      smooth_window=3).stoch_signal()
            else:
                # Для Ticks только RSI и MACD
                df["RSI"] = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
                df["MACD"] = ta.trend.MACD(df["Close"], window_slow=26, window_fast=12, window_sign=9).macd()

        # Сохранение обработанного файла
        output_path = os.path.join(output_dir, file.replace("_cleaned.csv", "_processed.csv"))
        df.to_csv(output_path, sep=",", index=False)
        print(f"Создан файл: {output_path}")
        print(f"Количество строк: {len(df)}")
        print(
            f"Пропуски в индикаторах:\n{df[['RSI', 'MACD', 'BB_High', 'BB_Low', 'ATR', 'Stochastic_K', 'Stochastic_D']].isna().sum() if 'Ticks' not in file else df[['RSI', 'MACD']].isna().sum()}")

print("Добавление индикаторов завершено.")