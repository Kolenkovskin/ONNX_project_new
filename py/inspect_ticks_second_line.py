# Метка: inspect_ticks_second_line_20250601_2338
# Дата и время запуска: 01 июня 2025, 23:38 EEST

import os
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Путь к файлу
file_path = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Ticks_2020.01.01_2025.05.25.csv"

# Проверка существования файла
if not os.path.exists(file_path):
    print(f"Ошибка: Файл {file_path} не найден")
    exit(1)

# Чтение второй строки
print("\nВторая строка файла (как в Блокноте):")
with open(file_path, 'r', encoding='utf-8') as f:
    # Пропустить первую строку (заголовки)
    f.readline()
    # Читать вторую строку
    second_line = f.readline().strip()
print(f"Строка 2: {second_line}")

# Вывод каждого символа второй строки
print("\nКаждый символ второй строки:")
for i, char in enumerate(second_line, 1):
    print(f"Символ {i}: '{char}'")

print("\nПроверка завершена.")