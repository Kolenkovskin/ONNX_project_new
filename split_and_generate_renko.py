# Метка: split_and_generate_renko_fixed_20250602_1945
# Дата и время запуска: 02 июня 2025, 19:45 EEST

import pandas as pd
import os
from datetime import datetime
import time

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Настройки
input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"
temp_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
log_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\logs\renko_errors.log"
os.makedirs(temp_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)
brick_size = 0.01  # 1 пункт = 0.01 для XAUUSD

# Проверка существования файла
if not os.path.exists(input_file):
    print(f"Ошибка: Файл {input_file} не найден")
    exit(1)


# Логирование ошибок
def log_error(message, line=None, file=None):
    with open(log_file, 'a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[{timestamp}] {message}")
        if file:
            f.write(f" | File: {file}")
        if line:
            f.write(f" | Line: {line}")
        f.write("\n")


# 1. Разбиение файла по годам
print("\nРазбиение файла по годам...")
start_time = time.time()
try:
    chunksize = 10_000_000  # Чтение по 10 млн строк
    chunk_reader = pd.read_csv(input_file, sep=";", chunksize=chunksize)
    year_files = {}

    for chunk_idx, chunk in enumerate(chunk_reader):
        try:
            # Пропускаем строки с заголовками
            chunk = chunk[chunk["Time (EET)"] != "Time (EET)"]
            # Преобразуем время с обработкой различных форматов
            chunk["Time (EET)"] = pd.to_datetime(chunk["Time (EET)"], format='mixed', errors='coerce')
            chunk = chunk.dropna(subset=["Time (EET)"])  # Удаляем строки с некорректными датами
            for year in range(2020, 2026):
                year_chunk = chunk[chunk["Time (EET)"].dt.year == year]
                if not year_chunk.empty:
                    year_file = os.path.join(temp_dir, f"XAUUSD_Ticks_{year}.csv")
                    year_chunk.to_csv(year_file, sep=";", mode='a', index=False, header=not os.path.exists(year_file))
                    if year_file not in year_files:
                        year_files[year] = year_file
        except Exception as e:
            log_error(f"Ошибка обработки chunk {chunk_idx + 1}: {str(e)}")
            continue
except Exception as e:
    print(f"Ошибка разбиения файла: {str(e)}")
    exit(1)

print(f"Время разбиения: {time.time() - start_time:.2f} секунд")
print(f"Созданы файлы: {list(year_files.values())}")


# 2. Генерация Renko для каждой части
def generate_renko_block(df, brick_size, price_col, prev_price=None, prev_time=None, prev_volume=0):
    renko_data = []
    if df.empty:
        return pd.DataFrame(renko_data), prev_price, prev_time, prev_volume

    current_price = prev_price if prev_price is not None else df[price_col].iloc[0]
    start_time = prev_time if prev_time is not None else df["Time (EET)"].iloc[0]
    volume = prev_volume

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
                "WickPrice": None,
                "OppositeWickPrice": None
            })

            current_price = new_price
            start_time = end_time
            volume = 0

    return pd.DataFrame(renko_data), current_price, start_time, volume


# Обработка файлов
ask_parts = []
bid_parts = []
prev_ask_price = None
prev_ask_time = None
prev_ask_volume = 0
prev_bid_price = None
prev_bid_time = None
prev_bid_volume = 0

for year, year_file in sorted(year_files.items()):
    print(f"\nОбработка файла: {year_file}")
    try:
        df = pd.read_csv(year_file, sep=";")
        # Пропускаем строки с заголовками
        df = df[df["Time (EET)"] != "Time (EET)"]
        df["Time (EET)"] = pd.to_datetime(df["Time (EET)"], format='mixed', errors='coerce')
        df = df.dropna(subset=["Time (EET)"])  # Удаляем некорректные даты
        for col in ["Ask", "Bid", "AskVolume", "BidVolume"]:
            df[col] = df[col].str.replace(",", ".").astype(float)

        # Renko для Ask
        renko_ask, prev_ask_price, prev_ask_time, prev_ask_volume = generate_renko_block(
            df, brick_size, "Ask", prev_ask_price, prev_ask_time, prev_ask_volume
        )
        if not renko_ask.empty:
            ask_parts.append(renko_ask)

        # Renko для Bid
        renko_bid, prev_bid_price, prev_bid_time, prev_bid_volume = generate_renko_block(
            df, brick_size, "Bid", prev_bid_price, prev_bid_time, prev_bid_volume
        )
        if not renko_bid.empty:
            bid_parts.append(renko_bid)

        print(f"Обработано строк: {len(df)}, Ask баров: {len(renko_ask)}, Bid баров: {len(renko_bid)}")
    except Exception as e:
        log_error(f"Ошибка обработки {year_file}: {str(e)}", file=year_file)
        continue

# 3. Объединение и сохранение
print("\nСохранение результатов...")
try:
    if ask_parts:
        renko_ask = pd.concat(ask_parts, ignore_index=True)
        output_ask = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv")
        renko_ask.to_csv(output_ask, sep=";", index=False)
        print(f"Сохранен файл: {output_ask}, строк: {len(renko_ask)}")
    else:
        print("Нет данных для Renko Ask")

    if bid_parts:
        renko_bid = pd.concat(bid_parts, ignore_index=True)
        output_bid = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv")
        renko_bid.to_csv(output_bid, sep=";", index=False)
        print(f"Сохранен файл: {output_bid}, строк: {len(renko_bid)}")
    else:
        print("Нет данных для Renko Bid")
except Exception as e:
    log_error(f"Ошибка сохранения результатов: {str(e)}")

# Удаление временных файлов
print("\nУдаление временных файлов...")
for year_file in year_files.values():
    try:
        os.remove(year_file)
    except Exception as e:
        log_error(f"Ошибка удаления {year_file}: {str(e)}")

print(f"\nГенерация Renko завершена. Общее время: {time.time() - start_time:.2f} секунд")