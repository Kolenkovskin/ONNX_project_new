# Метка: add_indicators_renko_v1
# Дата и время запуска: 2025-06-22 22:28:00
# Ожидаемое время выполнения: ~1800 секунд (~30 минут)
# Версия: add_indicators_renko.py v1

import pandas as pd
import pandas_ta as ta
import numpy as np
import os
from datetime import datetime
import time

print(f"[{datetime.now()}] Скрипт add_indicators_renko_v1 запущен")


def calculate_fibonacci_levels(df, window=50):
    """Вычисление уровней Фибоначчи на основе максимума и минимума за окно."""
    high = df['High'].rolling(window=window).max()
    low = df['Low'].rolling(window=window).min()
    diff = high - low
    fib_levels = {
        'Fib_0_0': low,
        'Fib_23_6': low + diff * 0.236,
        'Fib_38_2': low + diff * 0.382,
        'Fib_50_0': low + diff * 0.5,
        'Fib_61_8': low + diff * 0.618,
        'Fib_100_0': high
    }
    return pd.DataFrame(fib_levels)


def detect_candlestick_patterns(df):
    """Обнаружение свечных паттернов с использованием pandas_ta."""
    patterns = ta.cdl_pattern(df['Open'], df['High'], df['Low'], df['Close'])
    return patterns[[col for col in patterns.columns if col.startswith('CDL')]]


def add_indicators_to_renko(file_path, output_dir):
    """Добавление индикаторов и свечных паттернов к Renko-файлу."""
    print(f"[{datetime.now()}] Обработка файла: {file_path}")
    try:
        chunksize = 1000000
        output_file = os.path.join(output_dir, os.path.basename(file_path).replace('.csv', '_indicators.csv'))

        first_chunk = True
        for chunk in pd.read_csv(file_path, delimiter=';', encoding='utf-8-sig', chunksize=chunksize):
            # Преобразование времени
            chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
            chunk['EndTime'] = pd.to_datetime(chunk['EndTime'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
            if chunk['Time (EET)'].isna().any() or chunk['EndTime'].isna().any():
                print(
                    f"[{datetime.now()}] Ошибка: Некорректный формат времени в {len(chunk[chunk['Time (EET)'].isna()])} строках")

            # Добавление индикаторов
            chunk['EMA_20'] = ta.ema(chunk['Close'], length=20)
            chunk['EMA_50'] = ta.ema(chunk['Close'], length=50)

            # VWAP
            typical_price = (chunk['High'] + chunk['Low'] + chunk['Close']) / 3
            chunk['VWAP'] = (typical_price * chunk['Volume']).cumsum() / chunk['Volume'].cumsum()

            # RSI
            chunk['RSI_14'] = ta.rsi(chunk['Close'], length=14)

            # ATR
            chunk['ATR_14'] = ta.atr(chunk['High'], chunk['Low'], chunk['Close'], length=14)

            # Фибоначчи
            fib_df = calculate_fibonacci_levels(chunk)
            chunk = pd.concat([chunk, fib_df], axis=1)

            # Свечные паттерны
            patterns_df = detect_candlestick_patterns(chunk)
            chunk = pd.concat([chunk, patterns_df], axis=1)

            # Сохранение
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            chunk.to_csv(output_file, sep=';', index=False, mode=mode, header=header)
            first_chunk = False

        print(f"[{datetime.now()}] Сохранён файл с индикаторами: {output_file}")
    except Exception as e:
        print(f"[{datetime.now()}] Ошибка обработки {file_path}: {e}")


# Основной код
start_time = time.time()
try:
    input_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_cleaned"
    output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"
    os.makedirs(output_dir, exist_ok=True)

    renko_files = [
        "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020_2025.csv",
        "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020_2025.csv"
    ]

    for file in renko_files:
        file_path = os.path.join(input_dir, file)
        if os.path.exists(file_path):
            add_indicators_to_renko(file_path, output_dir)
        else:
            print(f"[{datetime.now()}] Файл не найден: {file_path}")

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"[{datetime.now()}] Обработка завершена")
print(f"[{datetime.now()}] Генерация индикаторов завершена. Общее время: {time.time() - start_time:.2f} секунд")