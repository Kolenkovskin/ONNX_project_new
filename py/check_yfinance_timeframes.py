import yfinance as yf
from datetime import datetime

# Метка для подтверждения выполнения
print(f"[{datetime.now()}] Проверка доступных таймфреймов и периодов на yfinance для GC=F")

# Список таймфреймов и периодов для проверки
timeframes = ["1m", "5m", "15m", "1h", "1d"]
periods = ["1d", "7d", "1mo", "3mo", "1y", "max"]

# Символ для проверки (фьючерсы на золото)
symbol = "GC=F"

# Проверка каждого таймфрейма и периода
for tf in timeframes:
    print(f"\nТаймфрейм: {tf}")
    for period in periods:
        try:
            data = yf.download(symbol, period=period, interval=tf)
            if not data.empty:
                print(f"  Период {period}: Доступно. Размер данных: {len(data)} строк")
            else:
                print(f"  Период {period}: Данные отсутствуют")
        except Exception as e:
            print(f"  Период {period}: Ошибка - {str(e)}")