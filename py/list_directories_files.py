# list_directories_files.py
# Version: 1.0
# Launch Date: 2025-08-02 11:05:00
# Expected Execution Time: ~10 seconds

import os

def list_directory_contents(path):
    print(f"\nСодержимое {path}:")
    for root, dirs, files in os.walk(path):
        print(f"\nПапка: {root}")
        for dir_name in dirs:
            print(f"  Подпапка: {dir_name}")
        for file_name in files:
            print(f"  Файл: {file_name}")

# Список путей для проверки
paths = [
    r"C:\Strawberry",
    r"C:\Users\User\Downloads\ta-lib-0.6.4-windows-x86_64",
    r"C:\Users\User\Downloads\ta-lib-0.6.4-src"
]

# Проверка каждого пути
for path in paths:
    if os.path.exists(path):
        list_directory_contents(path)
    else:
        print(f"[2025-08-02 11:05:00] Путь не существует: {path}")

print(f"[2025-08-02 11:05:00] Сканирование завершено.")