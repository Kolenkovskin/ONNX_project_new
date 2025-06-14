# Метка: generate_renko_2020_test_v16
# Дата и время запуска: 2025-06-14 20:18:00

import pandas as pd
import os
import time
from datetime import datetime
import gc
import decimal

print(f"[{datetime.now()}] Скрипт запущен")

input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"
temp_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
brick_size = decimal.Decimal('0.1')  # Точный размер кирпича
chunksize = 100000  # Оптимизированный размер чанка
year = 2020  # Ограничение только 2020 годом
test_end = pd.Timestamp('2020-04-01 23:59:59')  # Тестовый период до 2020-04-01

os.makedirs(temp_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)


def split_by_quarter(chunk, temp_dir, year_files):
    chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f')
    chunk['Ask'] = chunk['Ask'].str.replace(',', '.').astype(float)
    chunk['Bid'] = chunk['Bid'].str.replace(',', '.').astype(float)
    chunk['AskVolume'] = chunk['AskVolume'].str.replace(',', '.').astype(float)
    chunk['BidVolume'] = chunk['BidVolume'].str.replace(',', '.').astype(float)

    # Ограничение только 2020 годом и тестовым периодом
    chunk_2020 = chunk[(chunk['Time (EET)'].dt.year == year) & (chunk['Time (EET)'] <= test_end)]
    if not chunk_2020.empty:
        q1_end = pd.Timestamp(f"{year}-03-31 23:59:59")
        q2_start = pd.Timestamp(f"{year}-04-01 00:00:00")
        q1_df = chunk_2020[chunk_2020['Time (EET)'] <= q1_end]
        q2_df = chunk_2020[chunk_2020['Time (EET)'] >= q2_start]

        if not q1_df.empty:
            temp_file_q1 = os.path.join(temp_dir, f"XAUUSD_Ticks_{year}_Q1.csv")
            q1_df.to_csv(temp_file_q1, mode='a', index=False, header=not os.path.exists(temp_file_q1))
            if temp_file_q1 not in year_files:
                year_files.append(temp_file_q1)
        if not q2_df.empty:
            temp_file_q2 = os.path.join(temp_dir, f"XAUUSD_Ticks_{year}_Q2.csv")
            q2_df.to_csv(temp_file_q2, mode='a', index=False, header=not os.path.exists(temp_file_q2))
            if temp_file_q2 not in year_files:
                year_files.append(temp_file_q2)


def generate_renko(df, brick_size, price_col, last_price=None, current_open=None):
    renko_data = []
    if last_price is None:
        min_price = decimal.Decimal(str(df[price_col].min()))
        max_price = decimal.Decimal(str(df[price_col].max()))
        initial_price = decimal.Decimal(str(df[price_col].iloc[0]))
        current_open = decimal.Decimal(str(round(float(initial_price) / float(brick_size)) * float(brick_size)))
        if abs(initial_price - current_open) > (brick_size / decimal.Decimal('2')):
            current_open += brick_size if initial_price > current_open else -brick_size
        print(
            f"Initial Renko: Initial Price = {initial_price}, Min Price = {min_price}, Max Price = {max_price}, Current Open = {current_open}")

    # Фильтрация дубликатов по времени и цене
    df = df.sort_values('Time (EET)').drop_duplicates(subset=['Time (EET)', price_col])

    for _, row in df.iterrows():
        price = decimal.Decimal(str(row[price_col]))
        if last_price is not None and abs(price - last_price) < decimal.Decimal('0.001'):  # Минимальное изменение
            continue
        price_diff = price - last_price if last_price else decimal.Decimal('0')
        if abs(price_diff) > decimal.Decimal('5'):  # Фильтр аномальных скачков
            print(f"Filtered: Large price diff | Price Diff: {price_diff}")
            continue
        num_bricks = int(abs(price_diff) / brick_size)
        print(f"Price: {price}, Last Price: {last_price}, Price Diff: {price_diff}, Num Bricks: {num_bricks}")

        if num_bricks > 0:
            direction = 1 if price_diff > 0 else -1
            for i in range(num_bricks):
                brick_open = current_open
                brick_close = brick_open + (brick_size * direction)
                if abs(brick_close - brick_open) != brick_size:  # Точная проверка шага
                    print(f"Error: Incorrect brick size | Close-Open: {abs(brick_close - brick_open)}")
                    continue
                renko_data.append({
                    'Time (EET)': row['Time (EET)'],
                    'EndTime': row['Time (EET)'],
                    'Open': float(brick_open.quantize(decimal.Decimal('0.1'))),
                    'High': float(max(brick_open, brick_close).quantize(decimal.Decimal('0.1'))),
                    'Low': float(min(brick_open, brick_close).quantize(decimal.Decimal('0.1'))),
                    'Close': float(brick_close.quantize(decimal.Decimal('0.1'))),
                    'Volume': float(row['AskVolume']) if price_col == 'Ask' else float(row['BidVolume'])
                })
                print(f"Added Brick: Open={brick_open}, Close={brick_close}, Time={row['Time (EET)']}")
                current_open = brick_close
            last_price = price

    return pd.DataFrame(renko_data), last_price, current_open


# Разбиение файла на кварталы
print(f"Разбиение файла: {input_file} для периода {year}-01-01 – {year}-04-01")
start_time = time.time()
year_files = []

for chunk in pd.read_csv(input_file, sep=';', chunksize=chunksize):
    split_by_quarter(chunk, temp_dir, year_files)

print(f"Время разбиения: {time.time() - start_time:.2f} секунд")
temp_files = sorted(year_files)

# Генерация Renko для каждого квартала с обработкой последнего чанка
for temp_file in temp_files:
    quarter = os.path.basename(temp_file).split('_')[-1].split('.')[0]
    print(f"Обработка файла: {temp_file} (Q{quarter})")

    last_ask_price = None
    last_bid_price = None
    current_ask_open = None
    current_bid_open = None

    for i, chunk in enumerate(pd.read_csv(temp_file, chunksize=chunksize)):
        chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'])
        chunk['Ask'] = chunk['Ask'].astype(float)
        chunk['Bid'] = chunk['Bid'].astype(float)

        # Renko для Ask
        ask_df, last_ask_price, current_ask_open = generate_renko(
            chunk, brick_size, 'Ask', last_ask_price, current_ask_open
        )
        if not ask_df.empty:
            output_file_ask = os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Ask_{year}_Q{quarter}.csv")
            ask_df.to_csv(output_file_ask, mode='a', index=False, header=not os.path.exists(output_file_ask))
            print(f"Сохранён файл: {output_file_ask}")

        # Renko для Bid
        bid_df, last_bid_price, current_bid_open = generate_renko(
            chunk, brick_size, 'Bid', last_bid_price, current_bid_open
        )
        if not bid_df.empty:
            output_file_bid = os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Bid_{year}_Q{quarter}.csv")
            bid_df.to_csv(output_file_bid, mode='a', index=False, header=not os.path.exists(output_file_bid))
            print(f"Сохранён файл: {output_file_bid}")

        # Очистка памяти после каждого чанка
        gc.collect()

    # Обработка оставшихся строк (если файл не делится нацело на chunksize)
    if i == 0:  # Если файл прочитан целиком в одном чанке
        with open(temp_file, 'r') as f:
            last_lines = f.readlines()[-100:]  # Берем последние 100 строк для проверки
        last_chunk = pd.read_csv(pd.io.common.StringIO(''.join(last_lines)),
                                 names=['Time (EET)', 'Ask', 'Bid', 'AskVolume', 'BidVolume'],
                                 dtype={'Ask': float, 'Bid': float, 'AskVolume': float, 'BidVolume': float})
        last_chunk['Time (EET)'] = pd.to_datetime(last_chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f')
        if not last_chunk.empty:
            ask_df, last_ask_price, current_ask_open = generate_renko(
                last_chunk, brick_size, 'Ask', last_ask_price, current_ask_open
            )
            if not ask_df.empty:
                output_file_ask = os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Ask_{year}_Q{quarter}.csv")
                ask_df.to_csv(output_file_ask, mode='a', index=False, header=not os.path.exists(output_file_ask))
                print(f"Сохранён файл (последний чанк): {output_file_ask}")
            bid_df, last_bid_price, current_bid_open = generate_renko(
                last_chunk, brick_size, 'Bid', last_bid_price, current_bid_open
            )
            if not bid_df.empty:
                output_file_bid = os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Bid_{year}_Q{quarter}.csv")
                bid_df.to_csv(output_file_bid, mode='a', index=False, header=not os.path.exists(output_file_bid))
                print(f"Сохранён файл (последний чанк): {output_file_bid}")

# Удаление временных файлов
print("Удаление временных файлов...")
for temp_file in temp_files:
    if os.path.exists(temp_file):
        os.remove(temp_file)

print(f"Генерация Renko завершена. Общее время: {time.time() - start_time:.2f} секунд")