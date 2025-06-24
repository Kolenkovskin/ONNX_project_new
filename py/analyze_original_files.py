# Метка: check_original_data_v1
# Дата и время запуска: 2025-06-22 15:31:00
# Ожидаемое время выполнения: ~1800 секунд (~30 минут)

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import hashlib
import time

print(f"[{datetime.now()}] Скрипт check_original_data_v1 запущен")

# Список праздников (как в check_renko_integrity_v7)
HOLIDAYS = [
    pd.Timestamp('2020-01-01'), pd.Timestamp('2021-01-01'), pd.Timestamp('2022-01-01'),
    pd.Timestamp('2023-01-01'), pd.Timestamp('2024-01-01'), pd.Timestamp('2025-01-01'),
    pd.Timestamp('2020-12-25'), pd.Timestamp('2021-12-25'), pd.Timestamp('2022-12-25'),
    pd.Timestamp('2023-12-25'), pd.Timestamp('2024-12-25'),
    pd.Timestamp('2020-04-12'), pd.Timestamp('2021-04-04'), pd.Timestamp('2022-04-17'),
    pd.Timestamp('2023-04-09'), pd.Timestamp('2024-03-31'),
    pd.Timestamp('2020-11-26'), pd.Timestamp('2021-11-25'), pd.Timestamp('2022-11-24'),
    pd.Timestamp('2023-11-23'), pd.Timestamp('2024-11-28'),
]


def is_market_closed(start, end):
    """Проверка, является ли разрыв рыночной паузой (выходные или праздники)."""
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    if start_dt.weekday() == 4 and start_dt.hour >= 23 and end_dt.weekday() == 6 and end_dt.hour >= 23:
        return True
    for holiday in HOLIDAYS:
        if start_dt.date() <= holiday.date() <= end_dt.date():
            return True
    return False


def compute_file_hash(file_path):
    """Вычисление SHA256 хеша файла."""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        return f"Ошибка хеширования: {e}"


def check_file_quality(file_path):
    """Проверка качества данных в файле."""
    print(f"[{datetime.now()}] Проверка файла: {file_path}")
    result = {
        'file_path': file_path,
        'size_mb': os.path.getsize(file_path) / 1024 ** 2,
        'total_rows': 0,
        'missing_values': 0,
        'duplicates': 0,
        'non_monotonic': False,
        'min_date': None,
        'max_date': None,
        'price_anomalies': 0,
        'volume_anomalies': 0,
        'gaps_count': 0,
        'gap_details': [],
        'columns': [],
        'stats': {},
        'format_errors': [],
        'first_rows': []
    }

    try:
        is_ticks = "Ticks" in file_path
        expected_cols = ['Time (EET)', 'Ask', 'Bid', 'AskVolume', 'BidVolume'] if is_ticks else ['Time (EET)', 'Open',
                                                                                                 'High', 'Low', 'Close',
                                                                                                 'Volume']
        chunksize = 1000000 if is_ticks else 100000
        first_chunk = True

        for chunk in pd.read_csv(file_path, delimiter=';', encoding='utf-8-sig', chunksize=chunksize):
            if first_chunk:
                result['columns'] = chunk.columns.tolist()
                print(f"[{datetime.now()}] Столбцы: {result['columns']}")
                missing_cols = [col for col in expected_cols if col not in result['columns']]
                if missing_cols:
                    result['format_errors'].append(f"Отсутствуют столбцы: {missing_cols}")
                # Сохранение первых 5 строк
                result['first_rows'] = chunk.head(5).to_dict('records')
                first_chunk = False

            result['total_rows'] += len(chunk)
            result['missing_values'] += chunk.isna().sum().sum()
            result['duplicates'] += chunk.duplicated().sum()

            # Проверка времени
            chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S', errors='coerce')
            if chunk['Time (EET)'].isna().any():
                result['format_errors'].append(
                    f"Некорректный формат времени в {len(chunk[chunk['Time (EET)'].isna()])} строках")
            if not chunk['Time (EET)'].is_monotonic_increasing:
                result['non_monotonic'] = True
            if result['min_date'] is None or (
                    chunk['Time (EET)'].min() < result['min_date'] and not pd.isna(chunk['Time (EET)'].min())):
                result['min_date'] = chunk['Time (EET)'].min()
            if result['max_date'] is None or (
                    chunk['Time (EET)'].max() > result['max_date'] and not pd.isna(chunk['Time (EET)'].max())):
                result['max_date'] = chunk['Time (EET)'].max()

            # Проверка аномалий цен
            price_cols = ['Ask', 'Bid'] if is_ticks else ['Open', 'High', 'Low', 'Close']
            for col in price_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col].str.replace(',', '.'), errors='coerce')
                    anomalies = ((chunk[col] < 0) | (chunk[col] > 10000)).sum()
                    result['price_anomalies'] += anomalies
                    if chunk[col].isna().any():
                        result['format_errors'].append(
                            f"Некорректный формат данных в столбце {col}: {len(chunk[chunk[col].isna()])} строк")

            # Проверка аномалий объемов
            volume_cols = ['AskVolume', 'BidVolume'] if is_ticks else ['Volume']
            for col in volume_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col].str.replace(',', '.'), errors='coerce')
                    volume_anomalies = ((chunk[col] < 0) | (chunk[col] > 10000)).sum()
                    result['volume_anomalies'] += volume_anomalies
                    if chunk[col].isna().any():
                        result['format_errors'].append(
                            f"Некорректный формат данных в столбце {col}: {len(chunk[chunk[col].isna()])} строк")

            # Проверка разрывов в датах
            time_diffs = chunk['Time (EET)'].diff().dropna()
            gaps = time_diffs[time_diffs > timedelta(hours=1)]
            for i, diff in zip(gaps.index, gaps):
                start = chunk['Time (EET)'][i - 1]
                end = chunk['Time (EET)'][i]
                if diff > timedelta(hours=1) and not is_market_closed(start, end):
                    result['gaps_count'] += 1
                    if len(result['gap_details']) < 10:
                        result['gap_details'].append((start, end, diff))

            # Статистика
            numeric_cols = price_cols + volume_cols
            for col in numeric_cols:
                if col in chunk.columns and col not in result['stats']:
                    result['stats'][col] = []
                if col in chunk.columns:
                    result['stats'][col].append(chunk[col].describe())

        # Объединение статистики
        for col in result['stats']:
            stats = pd.concat(result['stats'][col], axis=1).mean(axis=1)
            result['stats'][col] = {
                'mean': stats['mean'],
                'min': stats['min'],
                'max': stats['max'],
                'std': stats['std']
            }

        # Хеш файла
        result['file_hash'] = compute_file_hash(file_path)
        print(f"[{datetime.now()}] Хеш файла {file_path}: {result['file_hash']}")

        # Вывод результатов
        print(f"\n[{datetime.now()}] Результаты проверки {file_path}:")
        print(f"Размер: {result['size_mb']:.2f} МБ")
        print(f"Строк: {result['total_rows']}")
        print(f"Пропуски: {result['missing_values']}")
        print(f"Дубликаты: {result['duplicates']}")
        print(f"Монотонность времени: {not result['non_monotonic']}")
        print(f"Диапазон дат: {result['min_date']} – {result['max_date']}")
        print(f"Аномалии цен: {result['price_anomalies']}")
        print(f"Аномалии объемов: {result['volume_anomalies']}")
        print(f"Разрывы в датах (>1 часа): {result['gaps_count']}")
        if result['gap_details']:
            print("Детали разрывов (первые 10):")
            for start, end, diff in result['gap_details']:
                print(f"Разрыв с {start} до {end} ({diff})")
        print(f"Форматные ошибки: {result['format_errors']}")
        print(f"Статистика: {result['stats']}")
        print(f"Первые 5 строк: {result['first_rows']}")

        # Оценка необходимости очистки
        needs_cleaning = result['missing_values'] > 0 or result['duplicates'] > 0 or result['price_anomalies'] > 0 or \
                         result['volume_anomalies'] > 0 or result['non_monotonic'] or result['format_errors']
        print(f"Необходимость очистки: {needs_cleaning}")
        if needs_cleaning:
            reasons = []
            if result['missing_values'] > 0:
                reasons.append(f"Пропуски: {result['missing_values']}")
            if result['duplicates'] > 0:
                reasons.append(f"Дубликаты: {result['duplicates']}")
            if result['price_anomalies'] > 0:
                reasons.append(f"Аномалии цен: {result['price_anomalies']}")
            if result['volume_anomalies'] > 0:
                reasons.append(f"Аномалии объемов: {result['volume_anomalies']}")
            if result['non_monotonic']:
                reasons.append("Нарушение монотонности времени")
            if result['format_errors']:
                reasons.append(f"Форматные ошибки: {result['format_errors']}")
            print(f"Причины: {reasons}")

        # Оценка соответствия проекту
        is_suitable = not needs_cleaning and result['min_date'] <= pd.Timestamp('2020-01-02') and result[
            'max_date'] >= pd.Timestamp('2025-05-23')
        print(f"Соответствие проекту: {is_suitable}")
        if not is_suitable:
            reasons = []
            if needs_cleaning:
                reasons.append("Требуется очистка данных")
            if result['min_date'] > pd.Timestamp('2020-01-02'):
                reasons.append(f"Недостаточный начальный диапазон: {result['min_date']}")
            if result['max_date'] < pd.Timestamp('2025-05-23'):
                reasons.append(f"Недостаточный конечный диапазон: {result['max_date']}")
            print(f"Причины несоответствия: {reasons}")

    except Exception as e:
        result['format_errors'].append(f"Ошибка обработки файла: {e}")
        print(f"[{datetime.now()}] Ошибка в check_file_quality: {e}")

    return result


# Основной код
start_time = time.time()
try:
    original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
    expected_files = [
        "XAUUSD_1 Min_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_1 Min_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_30 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_30 Mins_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_Hourly_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_Hourly_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_Daily_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_Daily_Bid_2020.01.01_2025.05.25.csv",
        "XAUUSD_Ticks_2020.01.01_2025.05.25.csv"
    ]

    # Проверка наличия файлов
    print(f"[{datetime.now()}] Проверка наличия файлов:")
    missing_files = [f for f in expected_files if not os.path.exists(os.path.join(original_dir, f))]
    print(f"Отсутствующие файлы: {missing_files if missing_files else 'Все файлы присутствуют'}")

    for file in expected_files:
        if os.path.exists(os.path.join(original_dir, file)):
            file_path = os.path.join(original_dir, file)
            check_file_quality(file_path)

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"[{datetime.now()}] Проверка завершена")
print(f"[{datetime.now()}] Генерация проверки завершена. Общее время: {time.time() - start_time:.2f} секунд")