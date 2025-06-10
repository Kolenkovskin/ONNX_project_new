# Метка: check_renko_original_v2_20250601_1633
# Дата и время запуска: 01 июня 2025, 16:33 EEST

import pandas as pd
import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
files = [
    "XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv",
    "XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv"
]

for file in files:
    file_path = os.path.join(original_dir, file)
    if os.path.exists(file_path):
        print(f"\nФайл: {file}")
        df = pd.read_csv(file_path, sep=";")

        # Список числовых столбцов с учетом пробела
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "WickPrice", "OppositeWickPrice "]

        # Фильтрация только существующих столбцов
        available_numeric_cols = [col for col in numeric_cols if col in df.columns]

        # Преобразование числовых столбцов
        for col in available_numeric_cols:
            if df[col].dtype == object:
                try:
                    df[col] = df[col].str.replace(",", ".").astype(float)
                except Exception as e:
                    print(f"Ошибка преобразования столбца {col}: {str(e)}")

        # Статистика
        print("Описательная статистика для числовых столбцов:")
        print(df[available_numeric_cols].describe())

        # Период данных
        print("\nПериод данных:")
        print(f"Начало: {df['Time (EET)'].min()}")
        print(f"Конец: {df['Time (EET)'].max()}")

        # Первые 6 строк как в Блокноте
        print("\nПервые 6 строк (как в Блокноте):")
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                print(f"Строка {i}: {line.strip()}")
                if i == 6:
                    break
    else:
        print(f"\nФайл {file} не найден.")

print("\nПроверка завершена.")