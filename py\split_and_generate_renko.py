# Метка: generate_renko_2020_test_v3
# Дата и время запуска: 2025-06-09 21:55:00

import pandas as pd
import os
import time
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"
temp_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
brick_size = 0.1  # Размер кирпича Renko в пунктах
chunksize = 1000000  # Размер чанка
test_year = 2020  # Тестовый год

os.makedirs(temp_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)


def split_by_year(chunk, temp_dir, year_files):
    chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f')
    chunk['Ask'] = chunk['Ask'].str.replace(',', '.').astype(float)
    chunk['Bid'] = chunk['Bid'].str.replace(',', '.').astype(float)
    chunk['AskVolume'] = chunk['AskVolume'].str.replace(',', '.').astype(float)
    chunk['BidVolume'] = chunk['BidVolume'].str.replace(',', '.').astype(float)

    year = test_year
    year_df = chunk[chunk['Time (EET)'].dt.year == year]
    if not year_df.empty:
        temp_file = os.path.join(temp_dir, f"XAUUSD_Ticks_{year}.csv")
        year_df.to_csv(temp_file, mode='a', index=False, header=not os.path.exists(temp_file))
        if temp_file not in year_files:
            year_files.add(temp_file)


def generate_renko(df, brick_size, price_col, last_price=None, current_open=None):
    renko_data = []
    if last_price is None:
        last_price = df[price_col].iloc[0]
        current_open = round(last_price - (last_price % brick_size), 2)

    df = df.sort_values('Time (EET)')  # Гарантируем монотонность времени

    for _, row in df.iterrows():
        price = row[price_col]
        if price == last_price:
            continue

        price_diff = price - last_price
        num_bricks = int(round(abs(price_diff) / brick_size, 0))

        if num_bricks > 0:
            direction = 1 if price_diff > 0 else -1
            for i in range(num_bricks):
                brick_open = current_open
                brick_close = round(brick_open + (brick_size * direction), 2)
                brick_high = max(brick_open, brick_close)
                brick_low = min(brick_open, brick_close)
                renko_data.append({
                    'Time (EET)': row['Time (EET)'],
                    'EndTime': row['Time (EET)'],
                    'Open': brick_open,
                    'High': brick_high,
                    'Low': brick_low,
                    'Close': brick_close,
                    'Volume': row['AskVolume'] if price_col == 'Ask' else row['BidVolume']
                })
                current_open = brick_close
            last_price = price

    return pd.DataFrame(renko_data).drop_duplicates(), last_price, current_open


# Разбиение файла для тестового года
print(f"Разбиение файла: {input_file} для года {test_year}")
start_time = time.time()
year_files = set()

for chunk in pd.read_csv(input_file, sep=';', chunksize=chunksize):
    split_by_year(chunk, temp_dir, year_files)

print(f"Время разбиения: {time.time() - start_time:.2f} секунд")
temp_files = sorted(list(year_files))

# Генерация Renko для 2020 года
for temp_file in temp_files:
    year = os.path.basename(temp_file).split('_')[-1].split('.')[0]
    if int(year) != test_year:
        continue
    print(f"Обработка файла: {temp_file}")

    renko_ask_dfs = []
    renko_bid_dfs = []
    last_ask_price = None
    last_bid_price = None
    current_ask_open = None
    current_bid_open = None

    for chunk in pd.read_csv(temp_file, chunksize=chunksize):
        chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'])
        chunk['Ask'] = chunk['Ask'].astype(float)
        chunk['Bid'] = chunk['Bid'].astype(float)

        # Renko для Ask
        ask_df, last_ask_price, current_ask_open = generate_renko(
            chunk, brick_size, 'Ask', last_ask_price, current_ask_open
        )
        if not ask_df.empty:
            renko_ask_dfs.append(ask_df)

        # Renko для Bid
        bid_df, last_bid_price, current_bid_open = generate_renko(
            chunk, brick_size, 'Bid', last_bid_price, current_bid_open
        )
        if not bid_df.empty:
            renko_bid_dfs.append(bid_df)

    # Сохранение результатов
    print(f"Сохранение Renko для года {year}...")
    if renko_ask_dfs:
        final_renko_ask = pd.concat(renko_ask_dfs).drop_duplicates()
        final_renko_ask.to_csv(os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Ask_{year}.csv"), index=False)
    else:
        print(f"Нет данных для Renko Ask {year}")

    if renko_bid_dfs:
        final_renko_bid = pd.concat(renko_bid_dfs).drop_duplicates()
        final_renko_bid.to_csv(os.path.join(output_dir, f"XAUUSD_Renko_ONE_PIP_Ticks_Bid_{year}.csv"), index=False)
    else:
        print(f"Нет данных для Renko Bid {year}")

# Удаление временных файлов
print("Удаление временных файлов...")
for temp_file in temp_files:
    if os.path.exists(temp_file):
        os.remove(temp_file)

print(f"Генерация Renko завершена. Общее время: {time.time() - start_time:.2f} секунд")