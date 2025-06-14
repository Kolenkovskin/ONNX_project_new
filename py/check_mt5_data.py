# Метка: check_mt5_data_v1
# Дата и время запуска: 2025-06-13 18:43:00

import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime

print(f"[{datetime.now()}] Скрипт запущен")

# Инициализация подключения к MT5
if not mt5.initialize():
    print(f"Ошибка подключения к MT5: {mt5.last_error()}")
    quit()

# Параметры
symbol = "XAUUSD"
start_time = pd.to_datetime("2025-06-12 23:58:00")
end_time = pd.to_datetime("2025-06-13 18:17:00")
timezone = "EET"

# Получение тиковых данных
ticks = mt5.copy_ticks_range(symbol, start_time.to_pydatetime(), end_time.to_pydatetime(), mt5.COPY_TICKS_ALL)
if ticks is not None and len(ticks) > 0:
    df_ticks = pd.DataFrame(ticks)
    df_ticks['time'] = pd.to_datetime(df_ticks['time'], unit='s', utc=True).dt.tz_convert(timezone)
    print(f"Найдено {len(df_ticks)} тиковых данных за {start_time} – {end_time}")
    print(f"Диапазон тиков: {df_ticks['time'].min()} – {df_ticks['time'].max()}")
else:
    print(f"Тиковые данные за {start_time} – {end_time} отсутствуют")

# Получение M1-данных как альтернативу
rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, start_time.to_pydatetime(), end_time.to_pydatetime())
if rates is not None and len(rates) > 0:
    df_rates = pd.DataFrame(rates)
    df_rates['time'] = pd.to_datetime(df_rates['time'], unit='s', utc=True).dt.tz_convert(timezone)
    print(f"Найдено {len(df_rates)} M1-данных за {start_time} – {end_time}")
    print(f"Диапазон M1: {df_rates['time'].min()} – {df_rates['time'].max()}")
else:
    print(f"M1-данные за {start_time} – {end_time} отсутствуют")

# Определение максимально доступного интервала
max_available = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, 10000)  # 10,000 баров как пример
if max_available is not None and len(max_available) > 0:
    df_max = pd.DataFrame(max_available)
    df_max['time'] = pd.to_datetime(df_max['time'], unit='s', utc=True).dt.tz_convert(timezone)
    print(f"Максимально доступный интервал M1: {df_max['time'].min()} – {df_max['time'].max()}")
else:
    print("Не удалось определить максимально доступный интервал")

# Завершение подключения
mt5.shutdown()
print(f"[{datetime.now()}] Скрипт завершён")