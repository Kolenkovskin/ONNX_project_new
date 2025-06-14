import pandas as pd
from datetime import datetime

print(f"Скрипт запущен: {datetime.now()}")

# Загрузка данных
df_1m = pd.read_csv(r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\XAUUSD_1 Min_Ask_2020.01.01_2025.05.25_cleaned.csv")
print("Столбцы в XAUUSD_1 Min_Ask_2020.01.01_2025.05.25_cleaned.csv:")
print(df_1m.columns.tolist())

df_5m = pd.read_csv(r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25_cleaned.csv")
print("Столбцы в XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25_cleaned.csv:")
print(df_5m.columns.tolist())

print("Проверка завершена.")