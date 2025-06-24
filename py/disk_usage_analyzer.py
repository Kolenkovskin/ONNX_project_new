# Метка: disk_usage_analyzer_v1
# Дата и время запуска: 2025-06-20 21:09:00
# Ожидаемое время выполнения: ~300 секунд (зависит от объёма данных на диске)

import os
import heapq
from datetime import datetime
import time

print(f"[{datetime.now()}] Скрипт запущен")

def get_folder_size(folder_path):
    """Подсчёт размера папки в байтах."""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                try:
                    total_size += os.path.getsize(file_path)
                except (OSError, PermissionError):
                    pass
    except (OSError, PermissionError):
        pass
    return total_size

def get_file_size(file_path):
    """Получение размера файла в байтах."""
    try:
        return os.path.getsize(file_path)
    except (OSError, PermissionError):
        return 0

def human_readable_size(size_bytes):
    """Преобразование размера в читаемый формат (ГБ, МБ, КБ)."""
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} ПБ"

# Основной код
start_time = time.time()
try:
    root_dir = 'C:\\'
    folder_sizes = []
    file_sizes = []

    print(f"[{datetime.now()}] Сканирование диска {root_dir}...")

    # Сканирование диска
    for item in os.listdir(root_dir):
        item_path = os.path.join(root_dir, item)
        try:
            if os.path.isdir(item_path):
                size = get_folder_size(item_path)
                if size > 0:
                    heapq.heappush(folder_sizes, (-size, item_path))  # Минус для сортировки по убыванию
            elif os.path.isfile(item_path):
                size = get_file_size(item_path)
                if size > 0:
                    heapq.heappush(file_sizes, (-size, item_path))
        except (OSError, PermissionError):
            pass

    # Рекурсивное сканирование подкаталогов
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=True):
        try:
            for dirname in dirnames:
                folder_path = os.path.join(dirpath, dirname)
                size = get_folder_size(folder_path)
                if size > 0:
                    heapq.heappush(folder_sizes, (-size, folder_path))
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                size = get_file_size(file_path)
                if size > 0:
                    heapq.heappush(file_sizes, (-size, file_path))
        except (OSError, PermissionError):
            pass

    # Вывод 7 самых тяжёлых папок
    print(f"\n[{datetime.now()}] 7 самых тяжёлых папок на диске C:")
    top_folders = heapq.nsmallest(7, folder_sizes)
    for i, (neg_size, folder_path) in enumerate(top_folders, 1):
        size = -neg_size
        print(f"{i}. {folder_path}: {human_readable_size(size)}")

    # Вывод 10 самых тяжёлых файлов
    print(f"\n[{datetime.now()}] 10 самых тяжёлых файлов на диске C:")
    top_files = heapq.nsmallest(10, file_sizes)
    for i, (neg_size, file_path) in enumerate(top_files, 1):
        size = -neg_size
        print(f"{i}. {file_path}: {human_readable_size(size)}")

except Exception as e:
    print(f"[{datetime.now()}] Ошибка: {e}")

print(f"[{datetime.now()}] Сканирование завершено. Общее время: {time.time() - start_time:.2f} секунд")