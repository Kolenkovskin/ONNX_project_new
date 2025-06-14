# Метка: inspect_ticks_file_v2_20250601_2250
# Дата и время запуска: 01 июня 2025, 22:50 EEST

import dask.dataframe as dd
import os
from datetime import datetime
import collections

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Путь к файлу
file_path = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"

# Проверка существования файла
if not os.path.exists(file_path):
    print(f"Ошибка: Файл {file_path} не найден")
    exit(1)

# 1. Первые 6 строк как в Блокноте
print("\nПервые 6 строк (как в Блокноте):")
with open(file_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f, 1):
        print(f"Строка {i}: {line.strip()}")
        if i == 6:
            break

# 2. Последние 5 строк как в Блокноте
print("\nПоследние 5 строк (как в Блокноте):")
last_lines = collections.deque(maxlen=5)
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        last_lines.append(line.strip())

for i, line in enumerate(last_lines, 1):
    print(f"Строка {i}: {line}")

# Чтение файла с помощью dask
print(f"\nЧтение файла: {file_path}")
ddf = dd.read_csv(file_path, sep=";", dtype={"Ask": str, "Bid": str, "AskVolume": str, "BidVolume": str})

# 3. Наименование всех столбцов
print("\nНаименование всех столбцов:")
print(", ".join(ddf.columns))

# 4. Каждый символ первой строки (заголовков)
print("\nКаждый символ первой строки (заголовков):")
with open(file_path, 'r', encoding='utf-8') as f:
    first_line = f.readline().strip()
for i, char in enumerate(first_line, 1):
    print(f"Символ {i}: '{char}'")

# 5. Описательная статистика для числовых столбцов
print("\nОписательная статистика для числовых столбцов:")
numeric_cols = ["Ask", "Bid", "AskVolume", "BidVolume"]
for col in numeric_cols:
    if col in ddf.columns:
        print(f"\nСтолбец: {col}")
        # Преобразование строк с запятыми в числа
        ddf[col] = ddf[col].str.replace(",", ".").astype(float)
        stats = ddf[col].describe().compute()
        # Форматирование без научной нотации
        stats = stats.apply(lambda x: f"{x:.6f}")
        print(stats.to_string())

# Проверка периода
print("\nПроверка периода данных:")
ddf["Time (EET)"] = dd.to_datetime(ddf["Time (EET)"])
min_date = ddf["Time (EET)"].min().compute()
max_date = ddf["Time (EET)"].max().compute()
print(f"Начало периода: {min_date}")
print(f"Конец периода: {max_date}")

print("\nПроверка завершена.")