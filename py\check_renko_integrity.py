# Метка: check_renko_integrity_v1
# Дата и время запуска: 2025-06-09 18:05:00

import pandas as pd
import os
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
years = range(2020, 2026)  # 2020–2025
brick_size = 0.1  # Ожидаемый шаг Renko в пунктах

files = [
            os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Ask_{year}.csv")
            for year in years
        ] + [
            os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Bid_{year}.csv")
            for year in years
        ]

for file_path in files:
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        continue

    print(f"\nПроверка файла: {file_path}")
    file_size = os.path.getsize(file_path) / 1024 / 1024  # Размер в МБ
    print(f"Размер файла: {file_size:.2f} МБ")

    # Чтение файла с потоковой обработкой
    chunksize = 1000000
    df_chunks = pd.read_csv(file_path, chunksize=chunksize)
    total_rows = 0
    missing_values = 0
    duplicates = 0
    is_monotonic = True
    price_anomalies = 0
    step_anomalies = 0
    min_date = None
    max_date = None
    prev_close = None

    expected_columns = ['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume']

    for chunk in df_chunks:
        total_rows += len(chunk)

        # Проверка столбцов
        if not all(col in chunk.columns for col in expected_columns):
            print(f"Ошибка: отсутствуют ожидаемые столбцы {expected_columns}")
            break

        # Проверка типов данных
        chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], errors='coerce')
        chunk['EndTime'] = pd.to_datetime(chunk['EndTime'], errors='coerce')
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

        # Проверка пропусков
        missing_values += chunk.isnull().sum().sum()

        # Проверка дубликатов
        duplicates += chunk.duplicated().sum()

        # Проверка монотонности времени
        if not chunk['Time (EET)'].is_monotonic_increasing:
            is_monotonic = False

        # Проверка диапазона дат
        chunk_min = chunk['Time (EET)'].min()
        chunk_max = chunk['Time (EET)'].max()
        if min_date is None or chunk_min < min_date:
            min_date = chunk_min
        if max_date is None or chunk_max > max_date:
            max_date = chunk_max

        # Проверка аномалий в ценах
        for col in ['Open', 'High', 'Low', 'Close']:
            negative_prices = (chunk[col] < 0).sum()
            extreme_prices = (chunk[col] > 10000).sum()
            price_anomalies += negative_prices + extreme_prices

        # Проверка шага Renko
        if prev_close is not None:
            chunk['PriceStep'] = chunk['Close'] - chunk['Open'].shift(-1)
            step_anomalies += (chunk['PriceStep'].abs() != brick_size).sum()

        prev_close = chunk['Close'].iloc[-1]

    print(f"Общее количество строк: {total_rows}")
    print(f"Пропуски: {missing_values}")
    print(f"Дубликаты: {duplicates}")
    print(f"Монотонность времени: {'Да' if is_monotonic else 'Нет'}")
    print(f"Аномалии в ценах (отрицательные или >10000): {price_anomalies}")
    print(f"Аномалии в шаге Renko (не {brick_size}): {step_anomalies}")
    print(f"Диапазон дат: {min_date} – {max_date}")

print("Проверка завершена.")