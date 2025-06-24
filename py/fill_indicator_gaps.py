# Метка: fill_indicator_gaps_20250601_1528
# Дата и время запуска: 01 июня 2025, 15:28 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

input_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_processed"
indicators = ["RSI", "MACD", "BB_High", "BB_Low", "ATR", "Stochastic_K", "Stochastic_D"]

for file in os.listdir(input_dir):
    if file.endswith("_processed.csv"):
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path, sep=",")

        print(f"\nОбработка файла: {file}")
        initial_rows = len(df)

        # Обработка пропусков
        for indicator in indicators:
            if indicator in df.columns:
                if indicator in ["Stochastic_K", "Stochastic_D"] and ("1 Min" in file or "5 Mins" in file):
                    # Удаление строк с пропусками для Stochastic на 1 Min и 5 Mins
                    df = df.dropna(subset=[indicator])
                else:
                    # Линейная интерполяция для остальных индикаторов
                    df[indicator] = df[indicator].interpolate(method="linear", limit_direction="both")
                    # Заполнение оставшихся пропусков (если есть)
                    df[indicator] = df[indicator].bfill().ffill()

        # Проверка качества данных
        print(f"Количество строк после обработки: {len(df)}")
        print(f"Удалено строк: {initial_rows - len(df)}")
        print(
            f"Оставшиеся пропуски:\n{df[indicators].isna().sum() if indicators[0] in df.columns else 'Нет индикаторов'}")

        # Проверка логичности значений
        if "RSI" in df.columns:
            invalid_rsi = ((df["RSI"] < 0) | (df["RSI"] > 100)).sum()
            print(f"Некорректные значения RSI (вне 0–100): {invalid_rsi}")
        if "Stochastic_K" in df.columns:
            invalid_stoch = ((df["Stochastic_K"] < 0) | (df["Stochastic_K"] > 100)).sum()
            print(f"Некорректные значения Stochastic_K (вне 0–100): {invalid_stoch}")

        # Сохранение обновленного файла
        df.to_csv(file_path, sep=",", index=False)
        print(f"Обновлен файл: {file_path}")

print("Обработка пропусков завершена.")