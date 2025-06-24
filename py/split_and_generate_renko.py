# Метка: renko_generator_v9
# Дата и время запуска: 2025-06-21 21:18:00
# Ожидаемое время выполнения: ~22000 секунд (~6 часов)

import pandas as pd
import decimal
import os
import time
from datetime import datetime
import hashlib

print(f"[{datetime.now()}] Скрипт запущен")

# Глобальные константы
brick_size = decimal.Decimal('0.1')
chunksize = 100000
test_start = pd.Timestamp('2020-01-01 00:00:00')
test_end = pd.Timestamp('2025-05-25 23:59:59')
years = range(2020, 2026)  # 2020–2025
months = range(1, 13)  # 1–12

# Пути к файлам
input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"
temp_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
log_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\logs"

# Создание директорий
os.makedirs(temp_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)


def split_by_period(chunk, month_files):
    """Разбиение чанка по месяцам."""
    try:
        chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f')
        chunk['Ask'] = chunk['Ask'].str.replace(',', '.').astype(str)
        chunk['Bid'] = chunk['Bid'].str.replace(',', '.').astype(str)
        chunk['AskVolume'] = chunk['AskVolume'].str.replace(',', '.').astype(float)
        chunk['BidVolume'] = chunk['BidVolume'].str.replace(',', '.').astype(float)
    except Exception as e:
        print(f"[{datetime.now()}] Ошибка преобразования данных: {e}")
        return

    for year in years:
        for month in months:
            start_date = pd.Timestamp(f'{year}-{month:02d}-01 00:00:00')
            end_date = pd.Timestamp(
                f'{year}-{month:02d}-{pd.Timestamp(f"{year}-{month:02d}-01").days_in_month} 23:59:59') if year < 2025 or month < 5 else test_end
            if start_date <= test_end:
                chunk_period = chunk[(chunk['Time (EET)'] >= start_date) & (chunk['Time (EET)'] <= end_date)]

                if not chunk_period.empty:
                    temp_file = os.path.join(temp_dir, f"XAUUSD_Ticks_{year}_{month:02d}.csv")
                    chunk_period.to_csv(temp_file, mode='a', index=False, sep=';',
                                        header=['Time (EET)', 'Ask', 'Bid', 'AskVolume',
                                                'BidVolume'] if not os.path.exists(temp_file) else False)
                    if temp_file not in month_files:
                        month_files.append(temp_file)


def generate_renko(chunk, brick_size, price_col, current_open=None):
    """Генерация Renko для одного чанка."""
    try:
        if price_col not in chunk.columns:
            raise KeyError(f"Столбец '{price_col}' не найден. Доступные столбцы: {chunk.columns.tolist()}")

        renko_data = []
        if current_open is None:
            current_open = decimal.Decimal(str(chunk[price_col].iloc[0])).quantize(brick_size,
                                                                                   rounding=decimal.ROUND_DOWN)

        chunk = chunk.sort_values('Time (EET)').drop_duplicates(keep='first').reset_index(drop=True)

        for _, row in chunk.iterrows():
            price = decimal.Decimal(str(row[price_col]))
            current_time = row['Time (EET)']
            volume = row['AskVolume'] if price_col == 'Ask' else row['BidVolume']

            price_diff = price - current_open
            if abs(price_diff) >= brick_size:
                num_bricks = int(price_diff / brick_size)
                direction = 1 if num_bricks > 0 else -1
                num_bricks = abs(num_bricks)

                for _ in range(num_bricks):
                    brick_open = current_open
                    brick_close = brick_open + (brick_size * direction)
                    renko_data.append({
                        'Time (EET)': current_time,
                        'EndTime': current_time,
                        'Open': float(brick_open.quantize(decimal.Decimal('0.1'))),
                        'High': float(max(brick_open, brick_close).quantize(decimal.Decimal('0.1'))),
                        'Low': float(min(brick_open, brick_close).quantize(decimal.Decimal('0.1'))),
                        'Close': float(brick_close.quantize(decimal.Decimal('0.1'))),
                        'Volume': float(volume)
                    })
                    current_open = brick_close

        renko_df = pd.DataFrame(renko_data)
        return renko_df, current_open
    except Exception as e:
        print(f"[{datetime.now()}] Ошибка генерации Renko для {price_col}: {e}")
        return pd.DataFrame(), current_open


def merge_files(file_list, output_file):
    """Объединение файлов с проверкой целостности."""
    total_rows = 0
    merged_df = []
    min_date = None
    max_date = None
    hash_list = []

    for file in sorted(file_list):
        try:
            # Проверяем первые строки файла для диагностики
            with open(file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                print(f"[{datetime.now()}] Заголовки файла {file}: {first_line}")

            df = pd.read_csv(file, delimiter=';',
                             names=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'], header=0)
            total_rows += len(df)
            df['Time (EET)'] = pd.to_datetime(df['Time (EET)'], format='%Y-%m-%d %H:%M:%S.%f')
            merged_df.append(df)

            if min_date is None or df['Time (EET)'].min() < min_date:
                min_date = df['Time (EET)'].min()
            if max_date is None or df['Time (EET)'].max() > max_date:
                max_date = df['Time (EET)'].max()

            with open(file, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
                hash_list.append(file_hash)

        except Exception as e:
            print(f"[{datetime.now()}] Ошибка чтения файла {file}: {e}")
            return None, 0, None, None

    final_df = pd.concat(merged_df, ignore_index=True)
    final_df = final_df.sort_values('Time (EET)').drop_duplicates(keep='first').reset_index(drop=True)

    if len(final_df) != total_rows:
        print(
            f"[{datetime.now()}] Ошибка: Потеря данных при объединении. Ожидалось {total_rows} строк, получено {len(final_df)}")
        return None, total_rows, min_date, max_date

    if not final_df['Time (EET)'].is_monotonic_increasing:
        print(f"[{datetime.now()}] Ошибка: Нарушена монотонность времени в объединённом файле")
        return None, total_rows, min_date, max_date

    final_df.to_csv(output_file, index=False, sep=';')
    return final_df, total_rows, min_date, max_date


# Основной код
start_time = time.time()
try:
    # Очистка папки temp
    for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    month_files = []
    chunk_count = 0
    for chunk in pd.read_csv(input_file, chunksize=chunksize, delimiter=';'):
        chunk_count += 1
        split_by_period(chunk, month_files)

    if not month_files:
        raise ValueError("Не созданы временные файлы. Данные за 2020–2025 годы не найдены.")

    ask_files = []
    bid_files = []
    for month_file in sorted(set(month_files)):
        year, month = os.path.basename(month_file).split('_')[2:4]
        month = int(month.split('.')[0])
        print(f"[{datetime.now()}] Обработка {year}-{month:02d}")

        ask_output = os.path.join(temp_dir, f"XAUUSD_Renko_Ask_{year}_{month:02d}.csv")
        bid_output = os.path.join(temp_dir, f"XAUUSD_Renko_Bid_{year}_{month:02d}.csv")
        ask_files.append(ask_output)
        bid_files.append(bid_output)

        current_open_ask = None
        current_open_bid = None
        ask_data = []
        bid_data = []

        for chunk in pd.read_csv(month_file, chunksize=chunksize, delimiter=';',
                                 names=['Time (EET)', 'Ask', 'Bid', 'AskVolume', 'BidVolume'], header=0):
            chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y-%m-%d %H:%M:%S.%f')
            ask_df, current_open_ask = generate_renko(chunk, brick_size, 'Ask', current_open_ask)
            bid_df, current_open_bid = generate_renko(chunk, brick_size, 'Bid', current_open_bid)

            if not ask_df.empty:
                ask_data.append(ask_df)
            if not bid_df.empty:
                bid_data.append(bid_df)

        if ask_data:
            final_ask_df = pd.concat(ask_data).drop_duplicates().sort_values('Time (EET)').reset_index(drop=True)
            final_ask_df.to_csv(ask_output, index=False, sep=';',
                                header=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
            print(
                f"[{datetime.now()}] Сохранён Renko-файл для Ask {year}-{month:02d}: {ask_output}, {len(final_ask_df)} кирпичей")

        if bid_data:
            final_bid_df = pd.concat(bid_data).drop_duplicates().sort_values('Time (EET)').reset_index(drop=True)
            final_bid_df.to_csv(bid_output, index=False, sep=';',
                                header=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
            print(
                f"[{datetime.now()}] Сохранён Renko-файл для Bid {year}-{month:02d}: {bid_output}, {len(final_bid_df)} кирпичей")

        # Удаление временного месячного файла
        if os.path.exists(month_file):
            os.remove(month_file)

    # Объединение файлов
    print(f"[{datetime.now()}] Объединение Ask-файлов...")
    output_file_ask = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020_2025.csv")
    ask_df, ask_total_rows, ask_min_date, ask_max_date = merge_files(ask_files, output_file_ask)
    if ask_df is not None:
        print(
            f"[{datetime.now()}] Успех: Сохранён объединённый Ask-файл: {output_file_ask}, {len(ask_df)} кирпичей, покрытие времени: {(ask_df['Time (EET)'].max() - ask_df['Time (EET)'].min()).total_seconds():.2f} секунд")
    else:
        print(f"[{datetime.now()}] Ошибка объединения Ask-файлов")
        print(f"[{datetime.now()}] Временные файлы не удалены для отладки. Проверьте папку {temp_dir}")

    print(f"[{datetime.now()}] Объединение Bid-файлов...")
    output_file_bid = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020_2025.csv")
    bid_df, bid_total_rows, bid_min_date, bid_max_date = merge_files(bid_files, output_file_bid)
    if bid_df is not None:
        print(
            f"[{datetime.now()}] Успех: Сохранён объединённый Bid-файл: {output_file_bid}, {len(bid_df)} кирпичей, покрытие времени: {(bid_df['Time (EET)'].max() - bid_df['Time (EET)'].min()).total_seconds():.2f} секунд")
    else:
        print(f"[{datetime.now()}] Ошибка объединения Bid-файлов")
        print(f"[{datetime.now()}] Временные файлы не удалены для отладки. Проверьте папку {temp_dir}")

    # Удаление временных Renko-файлов только при успехе
    if ask_df is not None and bid_df is not None:
        print(f"[{datetime.now()}] Удаление временных Renko-файлов...")
        for file in ask_files + bid_files:
            if os.path.exists(file):
                os.remove(file)

    print(f"[{datetime.now()}] Успех: Обработано {chunk_count} чанков")

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")
    print(f"[{datetime.now()}] Временные файлы не удалены для отладки. Проверьте папку {temp_dir}")

print(f"[{datetime.now()}] Генерация Renko завершена. Общее время: {time.time() - start_time:.2f} секунд")