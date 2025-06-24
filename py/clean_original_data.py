# Метка: clean_original_data_v2
# Дата и время запуска: 2025-06-22 17:40:00
# Ожидаемое время выполнения: ~3600 секунд (~60 минут)

import pandas as pd
import os
from datetime import datetime
import time

print(f"[{datetime.now()}] Скрипт clean_original_data_v2 запущен")


def clean_file(file_path, output_dir):
    """Очистка и переформатирование файла."""
    print(f"[{datetime.now()}] Обработка файла: {file_path}")
    try:
        is_ticks = "Ticks" in file_path
        chunksize = 1000000 if is_ticks else 100000
        output_file = os.path.join(output_dir, os.path.basename(file_path).replace('.csv', '_cleaned.csv'))

        first_chunk = True
        for chunk in pd.read_csv(file_path, delimiter=';', encoding='utf-8-sig', chunksize=chunksize):
            # Переименование Volume
            if 'Volume ' in chunk.columns:
                chunk.rename(columns={'Volume ': 'Volume'}, inplace=True)

            # Преобразование времени
            if is_ticks:
                chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S.%f',
                                                     errors='coerce')
                chunk['Time (EET)'] = chunk['Time (EET)'].dt.strftime('%Y.%m.%d %H:%M:%S.%f')
            else:
                chunk['Time (EET)'] = pd.to_datetime(chunk['Time (EET)'], format='%Y.%m.%d %H:%M:%S', errors='coerce')
                chunk['Time (EET)'] = chunk['Time (EET)'].dt.strftime('%Y.%m.%d %H:%M:%S')

            if chunk['Time (EET)'].isna().any():
                print(
                    f"[{datetime.now()}] Ошибка: Некорректный формат времени в {len(chunk[chunk['Time (EET)'].isna()])} строках")

            # Преобразование числовых столбцов
            numeric_cols = ['Ask', 'Bid', 'AskVolume', 'BidVolume'] if is_ticks else ['Open', 'High', 'Low', 'Close',
                                                                                      'Volume']
            for col in numeric_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col].str.replace(',', '.'), errors='coerce')

            # Сохранение
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            chunk.to_csv(output_file, sep=';', index=False, mode=mode, header=header)
            first_chunk = False

        print(f"[{datetime.now()}] Сохранён очищенный файл: {output_file}")
    except Exception as e:
        print(f"[{datetime.now()}] Ошибка обработки {file_path}: {e}")


# Основной код
start_time = time.time()
try:
    original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
    output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_cleaned"
    os.makedirs(output_dir, exist_ok=True)

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

    for file in expected_files:
        file_path = os.path.join(original_dir, file)
        if os.path.exists(file_path):
            clean_file(file_path, output_dir)
        else:
            print(f"[{datetime.now()}] Файл не найден: {file_path}")

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"[{datetime.now()}] Очистка завершена")
print(f"[{datetime.now()}] Генерация очистки завершена. Общее время: {time.time() - start_time:.2f} секунд")