import pandas as pd
from datetime import datetime

print(f"Скрипт запущен: {datetime.now()}")

# Загрузка тиковых данных
df_ticks = pd.read_csv(r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\XAUUSD_Ticks_2024.01.01_2025.05.25_cleaned.csv")
print("Столбцы в XAUUSD_Ticks_2024.01.01_2025.05.25_cleaned.csv:")
print(df_ticks.columns.tolist())

print("Проверка завершена.")