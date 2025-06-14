# Метка: check_tick_integrity_v3
# Дата и время запуска: 2025-06-14 22:10:00
import gc

import pandas as pd
import os
from datetime import datetime
import decimal

print(f"[{datetime.now()}] Скрипт запущен")

input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"

# Проверка существования файла
if not os.path.exists(input_file):
    print(f"Файл не найден: {input_file}")
    print(f"[{datetime.now()}] Скрипт завершён")
    exit()

# Получение размера файла
total_size = os.path.getsize(input_file)
print(f"Размер файла: {total_size / 1024 / 1024:.2f} МБ")

# Подсчёт общего количества строк
total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8'))
print(f"Общее количество строк: {total_lines}")

# Итеративная проверка с небольшим chunksize
chunk_size = 100000
duplicates = 0
time_monotonic = True
price_anomalies = 0
min_time = None
max_time = None
last_time = None
price_changes = 0
total_rows = 0

for i, chunk in enumerate(pd.read_csv(input_file, sep=';', chunksize=chunk_size)):
    # Установка заголовков из первой строки
    if i == 0:
        chunk.columns = ['Time (EET)', 'Ask', 'Bid', 'AskVolume', 'BidVolume']
    else:
        chunk.columns = ['Time (EET)', 'Ask', 'Bid', 'AskVolume', 'BidVolume']

    # Преобразование типов
    chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f')
    chunk['Ask'] = chunk['Ask'].str.replace(',', '.').astype(float)
    chunk['Bid'] = chunk['Bid'].str.replace(',', '.').astype(float)
    chunk['AskVolume'] = chunk['AskVolume'].str.replace(',', '.').astype(float)
    chunk['BidVolume'] = chunk['BidVolume'].str.replace(',', '.').astype(float)

    # Пропуски и дубликаты
    duplicates += chunk.duplicated(subset=['Time (EET)', 'Ask', 'Bid']).sum()

    # Монотонность времени
    if len(chunk) > 1:
        time_series = chunk['Time (EET)']
        if last_time is not None and any(time_series <= last_time):
            time_monotonic = False
        last_time = time_series.iloc[-1]

    # Диапазон дат
    if min_time is None or chunk['Time (EET)'].min() < min_time:
        min_time = chunk['Time (EET)'].min()
    if max_time is None or chunk['Time (EET)'].max() > max_time:
        max_time = chunk['Time (EET)'].max()

    # Аномалии в ценах
    price_anomalies += ((chunk['Ask'] < 0) | (chunk['Bid'] < 0) |
                        (chunk['Ask'] > 10000) | (chunk['Bid'] > 10000)).sum()

    # Оценка изменений цены для Renko
    if len(chunk) > 1:
        ask_diff = chunk['Ask'].diff().abs().fillna(0)  # Обработка NaN
        bid_diff = chunk['Bid'].diff().abs().fillna(0)  # Обработка NaN
        price_changes += (ask_diff >= decimal.Decimal('0.1')).sum()
        price_changes += (bid_diff >= decimal.Decimal('0.1')).sum()
    total_rows += len(chunk)

    gc.collect()  # Очистка памяти

print(f"Дубликаты: {duplicates}")
print(f"Монотонность времени: {time_monotonic}")
print(f"Диапазон дат: {min_time} – {max_time}")
print(f"Аномалии в ценах (отрицательные или >10000): {price_anomalies}")
print(f"Частота изменений цены >= 0.1: {price_changes} из {total_rows} строк ({(price_changes / total_rows * 100):.2f}%)")
print(f"[{datetime.now()}] Проверка завершена")