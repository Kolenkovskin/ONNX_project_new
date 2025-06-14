# Метка: generate_renko_from_ticks_dask_20250602_0006
# Дата и время запуска: 02 июня 2025, 00:06 EEST

import dask.dataframe as dd
import pandas as pd
import os
from datetime import datetime
import time

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Настройки
input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
os.makedirs(output_dir, exist_ok=True)
brick_size = 0.01  # 1 пункт = 0.01 для XAUUSD

# Проверка существования файла
if not os.path.exists(input_file):
    print(f"Ошибка: Файл {input_file} не найден")
    exit(1)

# Чтение тиковых данных с помощью dask
start_time = time.time()
print(f"Чтение файла: {input_file}")
try:
    ddf = dd.read_csv(input_file, sep=";", dtype={"Ask": str, "Bid": str, "AskVolume": str, "BidVolume": str})
except Exception as e:
    print(f"Ошибка чтения файла: {str(e)}")
    exit(1)
print(f"Количество строк (приблизительно): {len(ddf)}")
print(f"Время чтения: {time.time() - start_time:.2f} секунд")

# Преобразование столбцов
try:
    ddf["Time (EET)"] = dd.to_datetime(ddf["Time (EET)"])
    numeric_cols = ["Ask", "Bid", "AskVolume", "BidVolume"]
    for col in numeric_cols:
        ddf[col] = ddf[col].str.replace(",", ".").astype(float)
except Exception as e:
    print(f"Ошибка преобразования данных: {str(e)}")
    exit(1)


# Генерация Renko для блока данных
def generate_renko_block(df, brick_size, price_col):
    renko_data = []
    if df.empty:
        return pd.DataFrame(renko_data)

    current_price = df[price_col].iloc[0]
    start_time = df["Time (EET)"].iloc[0]
    volume = 0
    wick_price = None
    opposite_wick_price = None

    for index, row in df.iterrows():
        price = row[price_col]
        curr_volume = row["AskVolume"] if price_col == "Ask" else row["BidVolume"]
        volume += curr_volume

        while abs(price - current_price) >= brick_size:
            if price > current_price:
                new_price = current_price + brick_size
                direction = 1
            else:
                new_price = current_price - brick_size
                direction = -1

            end_time = row["Time (EET)"]
            renko_data.append({
                "Time (EET)": start_time,
                "EndTime": end_time,
                "Open": current_price,
                "High": new_price if direction == 1 else current_price,
                "Low": current_price if direction == 1 else new_price,
                "Close": new_price,
                "Volume": volume,
                "WickPrice": wick_price,
                "OppositeWickPrice": opposite_wick_price
            })

            current_price = new_price
            start_time = end_time
            volume = 0
            wick_price = None
            opposite_wick_price = None

    return pd.DataFrame(renko_data)


# Генерация Renko для Ask
print("\nГенерация Renko для Ask")
start_time = time.time()
try:
    renko_ask_parts = []
    for partition in ddf.to_delayed():
        part_df = partition.compute()
        renko_part = generate_renko_block(part_df, brick_size, "Ask")
        if not renko_part.empty:
            renko_ask_parts.append(renko_part)

    if renko_ask_parts:
        renko_ask = pd.concat(renko_ask_parts, ignore_index=True)
        output_ask = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv")
        renko_ask.to_csv(output_ask, sep=";", index=False)
        print(f"Сохранен файл: {output_ask}, строк: {len(renko_ask)}")
    else:
        print("Нет данных для Renko Ask")
    print(f"Время обработки Ask: {time.time() - start_time:.2f} секунд")
except Exception as e:
    print(f"Ошибка генерации Renko для Ask: {str(e)}")
    exit(1)

# Генерация Renko для Bid
print("\nГенерация Renko для Bid")
start_time = time.time()
try:
    renko_bid_parts = []
    for partition in ddf.to_delayed():
        part_df = partition.compute()
        renko_part = generate_renko_block(part_df, brick_size, "Bid")
        if not renko_part.empty:
            renko_bid_parts.append(renko_part)

    if renko_bid_parts:
        renko_bid = pd.concat(renko_bid_parts, ignore_index=True)
        output_bid = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv")
        renko_bid.to_csv(output_bid, sep=";", index=False)
        print(f"Сохранен файл: {output_bid}, строк: {len(renko_bid)}")
    else:
        print("Нет данных для Renko Bid")
    print(f"Время обработки Bid: {time.time() - start_time:.2f} секунд")
except Exception as e:
    print(f"Ошибка генерации Renko для Bid: {str(e)}")
    exit(1)

print(f"\nГенерация Renko завершена. Общее время: {time.time() - start_time:.2f} секунд")