# Метка: CollectOrderBook_20250528_2149
# Дата и время запуска: 28 мая 2025, 21:49 EEST
import MetaTrader5 as mt5
import pandas as pd
import os
from datetime import datetime

# Инициализация MT5
if not mt5.initialize():
    print("MT5 инициализация не удалась")
    quit()

# Проверка доступных символов
symbols = mt5.symbols_get()
print("Доступные символы:", [s.name for s in symbols if "XAU" in s.name or "GOLD" in s.name])

# Параметры
symbol = "GOLD"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\orderbook"
output_file = os.path.join(output_dir, "orderbook_GOLD.csv")

# Проверка информации о символе
symbol_info = mt5.symbol_info(symbol)
if symbol_info is None:
    print(f"Символ {symbol} не найден")
    mt5.shutdown()
    quit()
elif not symbol_info.visible:
    print(f"Символ {symbol} не активен, активируем...")
    if not mt5.symbol_select(symbol, True):
        print(f"Не удалось активировать символ {symbol}")
        mt5.shutdown()
        quit()

# Создание директории
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Период с 2025.05.01
start_date = datetime(2025, 5, 1)  # Уменьшенный период
end_date = datetime.now()

# Получение тиковых данных
ticks = mt5.copy_ticks_range(symbol, start_date, end_date, mt5.COPY_TICKS_ALL)
if ticks is None or len(ticks) == 0:
    print("Ошибка получения данных или данных нет")
    mt5.shutdown()
    quit()

# Преобразование в DataFrame
df = pd.DataFrame(ticks)
df['time'] = pd.to_datetime(df['time'], unit='s')
df.to_csv(output_file, index=False)
print(f"Данные сохранены в {output_file}, строк: {len(df)}")

# Закрытие MT5
mt5.shutdown()