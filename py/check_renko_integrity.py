# Метка: check_renko_integrity_v10
# Дата и время запуска: 2025-06-14 19:15:00

import pandas as pd
import os
from datetime import datetime
import decimal

print(f"[{datetime.now()}] Скрипт запущен")

base_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
files = [f for f in os.listdir(base_dir) if f.startswith("XAUUSD_Renko_ONE_PIP_Ticks_") and f.endswith(".csv")]

for file_name in files:
    file_path = os.path.join(base_dir, file_name)
    print(f"Проверка файла: {file_path}")

    # Получение размера файла
    total_size = os.path.getsize(file_path)
    print(f"Размер файла: {total_size / 1024 / 1024:.2f} МБ")

    # Подсчёт общего количества строк
    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    print(f"Общее количество строк: {total_lines}")

    # Итеративная проверка с небольшим chunksize
    chunk_size = 10000
    missing = 0
    duplicates = 0
    renko_step_anomalies = 0
    price_anomalies = 0
    time_monotonic = True
    min_time = None
    max_time = None
    endtime_mismatch = 0

    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
        if i == 0:
            chunk.columns = chunk.columns  # Устанавливаем заголовки из первой строки
        else:
            chunk.columns = chunk.columns  # Повторяем для всех чанков

        # Пропуски
        missing += chunk.isnull().sum().sum()

        # Дубликаты
        duplicates += chunk.duplicated().sum()

        # Монотонность времени
        if len(chunk) > 1:
            time_series = pd.to_datetime(chunk['Time (EET)'])
            if not time_series.is_monotonic_increasing:
                time_monotonic = False

        # Диапазон дат
        if min_time is None or pd.to_datetime(chunk['Time (EET)'].min()) < min_time:
            min_time = pd.to_datetime(chunk['Time (EET)'].min())
        if max_time is None or pd.to_datetime(chunk['Time (EET)'].max()) > max_time:
            max_time = pd.to_datetime(chunk['Time (EET)'].max())

        # Проверка совпадения Time (EET) и EndTime
        endtime_mismatch += (chunk['Time (EET)'] != chunk['EndTime']).sum()

        # Аномалии в ценах
        price_anomalies += ((chunk['Open'] < 0) | (chunk['High'] < 0) | (chunk['Low'] < 0) | (chunk['Close'] < 0) |
                            (chunk['Open'] > 10000) | (chunk['High'] > 10000) | (chunk['Low'] > 10000) |
                            (chunk['Close'] > 10000)).sum()

        # Проверка шага Renko
        for j in range(1, len(chunk)):
            prev_close = decimal.Decimal(str(chunk['Close'].iloc[j - 1]))
            curr_open = decimal.Decimal(str(chunk['Open'].iloc[j]))
            expected_step = abs(prev_close - curr_open)
            if expected_step != decimal.Decimal('0.1'):
                renko_step_anomalies += 1

        # Проверка объёма
        volume_anomalies = (chunk['Volume'] <= 0) | (chunk['Volume'] > 1)
        if volume_anomalies.any():
            renko_step_anomalies += volume_anomalies.sum()  # Учитываем аномалии объёма как часть общей статистики

    print(f"Пропуски: {missing}")
    print(f"Дубликаты: {duplicates}")
    print(f"Монотонность времени: {time_monotonic}")
    print(f"Диапазон дат: {min_time} – {max_time}")
    print(f"Отладка: Минимальная дата (первые строки): {min_time}")
    print(f"Отладка: Максимальная дата (последние строки): {max_time}")
    print(f"Несовпадения Time (EET) и EndTime: {endtime_mismatch}")
    print(f"Аномалии в ценах (отрицательные или >10000): {price_anomalies}")
    print(f"Аномалии в шаге Renko и Volume (не 0.1 или некорректный объём): {renko_step_anomalies}")

print(f"[{datetime.now()}] Проверка завершена")