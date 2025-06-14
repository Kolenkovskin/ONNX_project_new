# Метка: check_ticks_coverage_v1
# Дата и время запуска: 2025-06-08 18:53:00

import pandas as pd
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"

print(f"Проверка файла: {input_file}")

# Чтение первых и последних строк с потоковой обработкой
chunksize = 1000000
first_rows = []
last_rows = []
total_rows = 0

for chunk in pd.read_csv(input_file, sep=';', chunksize=chunksize):
    total_rows += len(chunk)
    if not first_rows:
        first_rows = chunk.head(5).values.tolist()
    last_rows = chunk.tail(5).values.tolist()

print(f"Общее количество строк: {total_rows}")
print("\nПервые 5 строк (как в Блокноте):")
for idx, row in enumerate(first_rows, 1):
    row_str = ";".join(str(val) for val in row)
    print(f"Строка {idx}: {row_str}")

print("\nПоследние 5 строк (как в Блокноте):")
for idx, row in enumerate(last_rows, 1):
    row_str = ";".join(str(val) for val in row)
    print(f"Строка {idx}: {row_str}")

# Проверка диапазона дат
print("\nПроверка диапазона дат...")
df = pd.read_csv(input_file, sep=';', usecols=['Time (EET)'], chunksize=chunksize)
min_date = None
max_date = None
for chunk in df:
    chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f')
    chunk_min = chunk['Time (EET)'].min()
    chunk_max = chunk['Time (EET)'].max()
    if min_date is None or chunk_min < min_date:
        min_date = chunk_min
    if max_date is None or chunk_max > max_date:
        max_date = chunk_max

print(f"Минимальная дата: {min_date}")
print(f"Максимальная дата: {max_date}")
print("Проверка завершена.")