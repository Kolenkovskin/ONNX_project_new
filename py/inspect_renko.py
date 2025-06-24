# Метка: inspect_renko_v2
# Дата и время запуска: 2025-06-22 10:40:00
# Ожидаемое время выполнения: ~120 секунд

import pandas as pd
import os
from datetime import datetime
import time

print(f"[{datetime.now()}] Скрипт запущен")


def inspect_file(file_path):
    """Анализ содержимого Renko-файла."""
    print(f"Анализ файла: {file_path}")
    try:
        df = pd.read_csv(file_path, delimiter=';',
                         names=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'], header=0)

        # Первые 5 строк
        print("Первые 5 строк (как в Блокноте):")
        for i, row in df.head(5).iterrows():
            print(
                f"Строка {i + 1}: {row['Time (EET)']};{row['EndTime']};{row['Open']};{row['High']};{row['Low']};{row['Close']};{row['Volume']}")

        # 5 случайных строк
        print("\n5 случайных строк (как в Блокноте):")
        random_rows = df.sample(5, random_state=42)
        for i, (_, row) in enumerate(random_rows.iterrows(), 1):
            print(
                f"Строка {i}: {row['Time (EET)']};{row['EndTime']};{row['Open']};{row['High']};{row['Low']};{row['Close']};{row['Volume']}")

        # Последние 5 строк
        print("\nПоследние 5 строк (как в Блокноте):")
        for i, row in df.tail(5).iterrows():
            print(
                f"Строка {len(df) - 4 + i}: {row['Time (EET)']};{row['EndTime']};{row['Open']};{row['High']};{row['Low']};{row['Close']};{row['Volume']}")

    except Exception as e:
        print(f"Ошибка при анализе файла {file_path}: {e}")


# Основной код
start_time = time.time()
try:
    files = [
        r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020_2025.csv",
        r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020_2025.csv"
    ]
    for file_path in files:
        if os.path.exists(file_path):
            inspect_file(file_path)
        else:
            print(f"Файл не найден: {file_path}")

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"\nАнализ завершен.")
print(f"[{datetime.now()}] Генерация анализа завершена. Общее время: {time.time() - start_time:.2f} секунд")