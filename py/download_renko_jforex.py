# Метка: download_renko_jforex_v2_20250601_1943
# Дата и время запуска: 01 июня 2025, 19:43 EEST

import pyautogui
import time
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Настройки
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex"
os.makedirs(output_dir, exist_ok=True)

# Период
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 5, 25)

# Координаты элементов интерфейса (нужно настроить)
# Замените на реальные координаты, полученные с помощью pyautogui.position()
TOOLS_MENU_X, TOOLS_MENU_Y = 50, 50  # Меню "Tools"
HIST_DATA_X, HIST_DATA_Y = 100, 100  # "Historical Data Manager"
INSTRUMENT_X, INSTRUMENT_Y = 200, 200  # Поле выбора инструмента
TYPE_X, TYPE_Y = 300, 300  # Поле выбора типа (Renko)
PERIOD_X, PERIOD_Y = 400, 400  # Поле периода
START_DATE_X, START_DATE_Y = 500, 500  # Поле начала периода
END_DATE_X, END_DATE_Y = 600, 600  # Поле конца периода
EXPORT_X, EXPORT_Y = 700, 700  # Кнопка экспорта
SAVE_PATH_X, SAVE_PATH_Y = 800, 800  # Поле пути сохранения

# Настройка pyautogui
pyautogui.FAILSAFE = True  # Переместите курсор в верхний левый угол для остановки
pyautogui.PAUSE = 0.5  # Пауза между действиями


# Функция для ввода даты
def type_date(date):
    pyautogui.write(date.strftime("%d.%m.%Y"))
    time.sleep(0.5)


# Разбиение на трехмесячные интервалы
current_date = start_date
while current_date < end_date:
    chunk_end = min(current_date + relativedelta(months=3) - timedelta(days=1), end_date)

    # Формирование имени файла
    file_name_ask = f"XAUUSD_Renko_1P_Ask_{current_date.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}.csv"
    file_name_bid = f"XAUUSD_Renko_1P_Bid_{current_date.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}.csv"
    file_path_ask = os.path.join(output_dir, file_name_ask)
    file_path_bid = os.path.join(output_dir, file_name_bid)

    print(f"\nЗагрузка данных: {current_date.strftime('%Y-%m-%d')} – {chunk_end.strftime('%Y-%m-%d')}")

    for file_type, file_path in [("Ask", file_path_ask), ("Bid", file_path_bid)]:
        print(f"Обработка: {file_type}")

        # Открытие менеджера исторических данных
        pyautogui.click(TOOLS_MENU_X, TOOLS_MENU_Y)
        time.sleep(2)
        pyautogui.click(HIST_DATA_X, HIST_DATA_Y)
        time.sleep(3)

        # Выбор инструмента
        pyautogui.click(INSTRUMENT_X, INSTRUMENT_Y)
        pyautogui.hotkey("ctrl", "a")
        pyautogui.write("XAU/USD")
        pyautogui.press("enter")
        time.sleep(1)

        # Выбор типа (Renko 1 пункт)
        pyautogui.click(TYPE_X, TYPE_Y)
        pyautogui.hotkey("ctrl", "a")
        pyautogui.write("Renko 1 pip")
        pyautogui.press("enter")
        time.sleep(1)

        # Указание периода
        pyautogui.click(START_DATE_X, START_DATE_Y)
        pyautogui.hotkey("ctrl", "a")
        type_date(current_date)
        pyautogui.press("enter")
        time.sleep(1)

        pyautogui.click(END_DATE_X, END_DATE_Y)
        pyautogui.hotkey("ctrl", "a")
        type_date(chunk_end)
        pyautogui.press("enter")
        time.sleep(1)

        # Экспорт
        pyautogui.click(EXPORT_X, EXPORT_Y)
        time.sleep(2)
        pyautogui.click(SAVE_PATH_X, SAVE_PATH_Y)
        pyautogui.hotkey("ctrl", "a")
        pyautogui.write(file_path)
        pyautogui.press("enter")
        time.sleep(5)  # Ожидание сохранения файла

        # Проверка, что файл создан
        if os.path.exists(file_path):
            print(f"Сохранен файл: {file_path}")
        else:
            print(f"Ошибка: Файл {file_path} не был создан")

        # Закрытие окна экспорта
        pyautogui.press("esc")
        time.sleep(1)

    current_date += relativedelta(months=3)

print("\nЗагрузка завершена.")