# Метка: Check15MinGaps_20250529_2131
# Дата и время запуска: 29 мая 2025, 21:31 EEST
import os
import pandas as pd
from datetime import datetime, timedelta

# Пути к папкам
source_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
print(f"[{pd.Timestamp.now()}] Проверка пропусков в 15 Mins файлах")

# Проверка 15 Mins файлов
for file in ["XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25.csv", "XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25.csv"]:
    file_path = os.path.join(source_dir, file)
    print(f"\nОбработка файла: {file}")

    df_15min = pd.read_csv(file_path, sep=';')
    df_15min['Time (EET)'] = pd.to_datetime(df_15min['Time (EET)'])

    # Проверка пропусков во временных рядах
    time_diffs = df_15min['Time (EET)'].diff().dropna()
    expected_interval = timedelta(minutes=15)
    gaps = time_diffs[time_diffs > expected_interval]
    print(f"Пропуски во времени (больше ожидаемого интервала {expected_interval}): {len(gaps)}")
    print(f"Процент пропусков: {len(gaps) / len(time_diffs) * 100:.2f}%")

print("\nПроверка завершена")