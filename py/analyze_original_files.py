# Метка: analyze_original_files_20250601_1356
# Дата и время запуска: 01 июня 2025, 13:56 EEST

import pandas as pd
import os
from datetime import datetime
import numpy as np

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
expected_files = [
    "XAUUSD_1 Min_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_1 Min_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_30 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_30 Mins_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_Hourly_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_Hourly_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_Daily_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_Daily_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv",
    "XAUUSD_Ticks_2024.01.01_2025.05.25.csv"
]

# Проверка наличия файлов
print("\nПроверка наличия файлов:")
missing_files = [f for f in expected_files if not os.path.exists(os.path.join(original_dir, f))]
print(f"Отсутствующие файлы: {missing_files if missing_files else 'Все файлы присутствуют'}")

for file in os.listdir(original_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(original_dir, file)
        print(f"\nАнализ файла: {file}")

        # Размер файла
        file_size = os.path.getsize(file_path)
        print(f"Размер: {file_size} байт")

        try:
            # Чтение файла с учетом возможных запятых в числах
            df = pd.read_csv(file_path, sep=";", thousands=None)

            # Столбцы
            columns = list(df.columns)
            print(f"Столбцы: {columns}")

            # Проверка ожидаемых столбцов
            expected_cols = ["Time (EET)", "Open", "High", "Low", "Close", "Volume"] if "Ticks" not in file else [
                "Time (EET)", "Ask", "Bid", "AskVolume", "BidVolume"]
            if "Renko" in file:
                expected_cols.extend(["EndTime", "WickPrice", "OppositeWickPrice"])
            missing_cols = [col for col in expected_cols if col not in columns]
            print(f"Отсутствующие столбцы: {missing_cols if missing_cols else 'Все присутствуют'}")

            # Проверка типов данных
            print("Типы данных:")
            for col in columns:
                print(f"{col}: {df[col].dtype}")

            # Преобразование числовых столбцов
            numeric_cols = [col for col in columns if
                            col not in ["Time (EET)", "EndTime", "WickPrice", "OppositeWickPrice"]]
            for col in numeric_cols:
                if df[col].dtype == object:
                    try:
                        df[col] = df[col].str.replace(",", ".").astype(float)
                        print(f"Преобразован столбец {col} из строк в float")
                    except Exception as e:
                        print(f"Ошибка преобразования столбца {col}: {str(e)}")

            # Пропуски
            print(f"Пропуски:\n{df.isna().sum()}")

            # Дубликаты
            duplicates = df.duplicated().sum()
            print(f"Дубликаты: {duplicates}")

            # Аномалии в числовых столбцах
            print("Аномалии:")
            for col in numeric_cols:
                if df[col].dtype in [np.float64, np.int64]:
                    negative_count = (df[col] < 0).sum()
                    extreme_count = (df[col] > 10000).sum()  # Произвольный порог для XAUUSD
                    print(f"{col} - Отрицательные значения: {negative_count}, Экстремальные (>10000): {extreme_count}")

            # Проверка формата чисел (запятые)
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                second_line = f.readline().strip()
                if "," in second_line and "." not in second_line.split(";")[1:]:
                    print("Обнаружены запятые вместо точек в числовых значениях")

            # Проверка временной последовательности
            if "Time (EET)" in df.columns:
                try:
                    df["Time (EET)"] = pd.to_datetime(df["Time (EET)"])
                    is_monotonic = df["Time (EET)"].is_monotonic_increasing
                    print(f"Временная последовательность монотонна: {is_monotonic}")
                    if not is_monotonic:
                        print(
                            f"Обнаружены нарушения порядка времени: {df['Time (EET)'][~df['Time (EET)'].shift().le(df['Time (EET)'])].count()} случаев")
                except Exception as e:
                    print(f"Ошибка проверки времени: {str(e)}")

        except Exception as e:
            print(f"Ошибка при анализе файла {file}: {str(e)}")

print("Анализ завершен.")