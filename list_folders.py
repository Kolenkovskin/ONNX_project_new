# Метка: ListFolders_20250530_0502
# Дата и время запуска: 30 мая 2025, 05:02 EEST
import os
from datetime import datetime

# Путь к директории проекта
project_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot"

print(f"[{datetime.now()}] Список папок в {project_dir}")

# Проверка существования директории
if not os.path.exists(project_dir):
    raise Exception(f"Директория {project_dir} не найдена")

# Получение списка всех папок в project_dir
folders = [f for f in os.listdir(project_dir) if os.path.isdir(os.path.join(project_dir, f))]

# Вывод списка папок
if folders:
    print("Найдены следующие папки:")
    for folder in folders:
        print(f" - {folder}")
else:
    print("Папки не найдены")

print("\nСканирование завершено")