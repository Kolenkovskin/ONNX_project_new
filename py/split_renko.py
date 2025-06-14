# Метка: SplitRenkoAskBid_20250528_2114
# Дата и время запуска: 28 мая 2025, 21:14 EEST
import os
import shutil
import pandas as pd

# Папки
source_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
split_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\split_files\renko"
ask_dir = os.path.join(split_dir, "ask")
bid_dir = os.path.join(split_dir, "bid")

# Файлы
ask_file = os.path.join(source_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv")
bid_file = os.path.join(source_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv")

# 1. Удаление всех файлов из папки renko
if os.path.exists(split_dir):
    for file in os.listdir(split_dir):
        file_path = os.path.join(split_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
else:
    os.makedirs(split_dir)

# 2. Создание подпапок bid и ask
os.makedirs(ask_dir, exist_ok=True)
os.makedirs(bid_dir, exist_ok=True)

# 3. Копирование файлов
shutil.copy(ask_file, ask_dir)
shutil.copy(bid_file, bid_dir)

# 4. Разбиение файлов на части (примерно 50 КБ)
chunk_size = 500  # Примерное количество строк для файла ~50 КБ

# Разбиение Ask файла
ask_path = os.path.join(ask_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv")
df_ask = pd.read_csv(ask_path, sep=';')
for i in range(0, len(df_ask), chunk_size):
    chunk = df_ask[i:i + chunk_size]
    chunk.to_csv(os.path.join(ask_dir, f"XAUUSD_Renko_Ask_part_{i//chunk_size + 1}.csv"), index=False, sep=';')
    print(f"Создан файл: XAUUSD_Renko_Ask_part_{i//chunk_size + 1}.csv")
os.remove(ask_path)

# Разбиение Bid файла
bid_path = os.path.join(bid_dir, "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv")
df_bid = pd.read_csv(bid_path, sep=';')
for i in range(0, len(df_bid), chunk_size):
    chunk = df_bid[i:i + chunk_size]
    chunk.to_csv(os.path.join(bid_dir, f"XAUUSD_Renko_Bid_part_{i//chunk_size + 1}.csv"), index=False, sep=';')
    print(f"Создан файл: XAUUSD_Renko_Bid_part_{i//chunk_size + 1}.csv")
os.remove(bid_path)

print("Разбиение завершено")