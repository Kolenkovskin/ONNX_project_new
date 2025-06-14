import MetaTrader5 as mt5
from datetime import datetime

# Метка для подтверждения выполнения
print(f"[{datetime.now()}] Проверка доступа к стакану цен через MT5")

# Инициализация MT5
if not mt5.initialize():
    print("Ошибка инициализации MT5")
    quit()

# Проверка доступных символов
symbols = mt5.symbols_get()
for symbol in symbols:
    if symbol.name == "GOLD":
        print(f"Символ XAUUSD найден: {symbol.name}")
        # Получение данных стакана (10 уровней)
        book = mt5.market_book_get(symbol.name)
        if book:
            print(f"Стакан цен для XAUUSD: {book}")
        else:
            print("Стакан цен недоступен")
        break
else:
    print("Символ XAUUSD не найден")

# Завершение
mt5.shutdown()