# organize_data_reworked.py
# Уникальная метка: ORGANIZE_DATA_REWORKED_V1
# Дата и время запуска: 2025-06-01 12:01:00 EEST

import os
import shutil


def organize_files(directory):
    print(f"\nОрганизация файлов в папке: {directory}")

    # Создание подкаталогов
    processed_dir = os.path.join(directory, 'data_reworked_processed')
    other_dir = os.path.join(directory, 'data_reworked_other')
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(other_dir, exist_ok=True)

    # Список ожидаемых processed файлов
    processed_files = [
        'XAUUSD_1 Min_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_1 Min_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_30 Mins_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_30 Mins_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Hourly_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Hourly_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Daily_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Daily_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25_processed.csv',
        'XAUUSD_Ticks_2024.01.01_2025.05.25_processed.csv'
    ]

    # Перемещение файлов
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        if os.path.isfile(file_path):
            if file in processed_files:
                # Перемещение processed файлов
                dest_path = os.path.join(processed_dir, file)
                shutil.move(file_path, dest_path)
                print(f"Перемещён: {file} → {processed_dir}")
            elif file == 'XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25_cleaned.csv.tmp':
                # Удаление временного файла
                os.remove(file_path)
                print(f"Удалён: {file}")
            else:
                # Перемещение остальных файлов (merged, cleaned)
                dest_path = os.path.join(other_dir, file)
                shutil.move(file_path, dest_path)
                print(f"Перемещён: {file} → {other_dir}")


if __name__ == "__main__":
    directory = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
    organize_files(directory)