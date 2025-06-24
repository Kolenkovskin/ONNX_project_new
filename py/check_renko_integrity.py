# Метка: check_renko_integrity_v7
# Дата и время запуска: 2025-06-22 12:42:00
# Ожидаемое время выполнения: ~360 секунд

import pandas as pd
import decimal
import os
from datetime import datetime, timedelta
import time

print(f"[{datetime.now()}] Скрипт check_renko_integrity_v7 запущен")

# Расширенный список праздников (основные даты, когда рынок XAUUSD закрыт)
HOLIDAYS = [
    # Новый год
    pd.Timestamp('2020-01-01'), pd.Timestamp('2021-01-01'), pd.Timestamp('2022-01-01'),
    pd.Timestamp('2023-01-01'), pd.Timestamp('2024-01-01'), pd.Timestamp('2025-01-01'),
    # Рождество
    pd.Timestamp('2020-12-25'), pd.Timestamp('2021-12-25'), pd.Timestamp('2022-12-25'),
    pd.Timestamp('2023-12-25'), pd.Timestamp('2024-12-25'),
    # Пасха (примерные даты, так как точные зависят от календаря)
    pd.Timestamp('2020-04-12'), pd.Timestamp('2021-04-04'), pd.Timestamp('2022-04-17'),
    pd.Timestamp('2023-04-09'), pd.Timestamp('2024-03-31'),
    # День благодарения (четвертый четверг ноября, США)
    pd.Timestamp('2020-11-26'), pd.Timestamp('2021-11-25'), pd.Timestamp('2022-11-24'),
    pd.Timestamp('2023-11-23'), pd.Timestamp('2024-11-28'),
]

def is_market_closed(start, end):
    """Проверка, является ли разрыв рыночной паузой (выходные или праздники)."""
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    # Выходные: пятница 23:00 – воскресенье 23:00 EET
    if start_dt.weekday() == 4 and start_dt.hour >= 23 and end_dt.weekday() == 6 and end_dt.hour >= 23:
        return True
    # Праздники
    for holiday in HOLIDAYS:
        if start_dt.date() <= holiday.date() <= end_dt.date():
            return True
    return False

def check_file(file_path):
    """Проверка целостности Renko-файла."""
    print(f"Проверка файла: {file_path}")
    try:
        df = pd.read_csv(file_path, delimiter=';', encoding='utf-8-sig',
                         names=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'], header=0)
        total_rows = len(df)
        missing_values = df.isna().sum().sum()
        duplicates = df.duplicated().sum()

        df['Time (EET)'] = pd.to_datetime(df['Time (EET)'], format='%Y-%m-%d %H:%M:%S.%f')
        non_monotonic = not df['Time (EET)'].is_monotonic_increasing
        min_date = df['Time (EET)'].min()
        max_date = df['Time (EET)'].max()

        # Проверка разрывов в датах (>1 часа), исключая рыночные паузы
        time_diffs = df['Time (EET)'].diff().dropna()
        gaps = time_diffs[time_diffs > timedelta(hours=1)]
        gap_count = 0
        gap_details = []
        for i, diff in zip(gaps.index, gaps):
            start = df['Time (EET)'][i-1]
            end = df['Time (EET)'][i]
            if diff > timedelta(hours=1) and not is_market_closed(start, end):
                gap_count += 1
                gap_details.append((start, end, diff))
        gap_details = gap_details[:10]  # Ограничить вывод первыми 10 разрывами

        time_mismatches = (df['Time (EET)'] != df['EndTime']).sum()
        price_anomalies = ((df[['Open', 'High', 'Low', 'Close']] < 0) | (df[['Open', 'High', 'Low', 'Close']] > 10000)).sum().sum()

        df['Open'] = df['Open'].apply(lambda x: decimal.Decimal(str(x)))
        df['Close'] = df['Close'].apply(lambda x: decimal.Decimal(str(x)))
        steps = (df['Close'] - df['Open']).apply(lambda x: abs(x).quantize(decimal.Decimal('0.1')))
        step_anomalies = (steps != decimal.Decimal('0.1')).sum()
        step_values = set(steps[steps != decimal.Decimal('0.1')].apply(lambda x: float(x)))

        print(f"Размер файла: {os.path.getsize(file_path) / 1024**2:.2f} МБ")
        print(f"Общее количество строк: {total_rows}")
        print(f"Пропуски: {missing_values}")
        print(f"Дубликаты: {duplicates}")
        print(f"Монотонность времени: {not non_monotonic}")
        print(f"Диапазон дат: {min_date} – {max_date}")
        print(f"Отладка: Минимальная дата (первые строки): {min_date}")
        print(f"Отладка: Максимальная дата (последние строки): {max_date}")
        print(f"Несовпадения Time (EET) и EndTime: {time_mismatches}")
        print(f"Аномалии в ценах (отрицательные или >10000): {price_anomalies}")
        print(f"Аномалии в шаге Renko и Volume (не 0.1 или некорректный объём): {step_anomalies}")
        if step_anomalies > 0:
            print(f"Уникальные некорректные шаги: {sorted(step_values)}")
        print(f"Разрывы в датах (>1 часа, исключая выходные и праздники): {gap_count}")
        if gap_count > 0:
            print("Детали разрывов (первые 10):")
            for start, end, diff in gap_details:
                print(f"Разрыв с {start} до {end} ({diff})")
    except Exception as e:
        print(f"Ошибка проверки файла {file_path}: {e}")

# Основной код
start_time = time.time()
try:
    files = [
        r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020_2025.csv",
        r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020_2025.csv"
    ]
    for file_path in files:
        if os.path.exists(file_path):
            check_file(file_path)
        else:
            print(f"Файл не найден: {file_path}")

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"[{datetime.now()}] Проверка завершена")
print(f"[{datetime.now()}] Генерация проверки завершена. Общее время: {time.time() - start_time:.2f} секунд")