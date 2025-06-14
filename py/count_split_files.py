# Метка: count_split_files_20250525_2321
# Запуск: 2025-05-25 23:21 EEST

import os

# Путь к папке split_files
split_files_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\split_files"

# Инициализация общего счётчика
total_files = 0

# Обходим все папки и подпапки
for root, dirs, files in os.walk(split_files_dir):
    # Подсчитываем количество файлов в текущей папке
    file_count = len([f for f in files if f.endswith(".csv")])
    # Выводим путь к папке и количество файлов
    print(f"Папка: {root}, Количество файлов: {file_count}")
    total_files += file_count

# Выводим общее количество файлов
print(f"\nОбщее количество файлов во всех папках: {total_files}")

print("Подсчёт завершён.")