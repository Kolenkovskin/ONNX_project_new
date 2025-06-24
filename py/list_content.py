import os
import pkg_resources
from datetime import datetime

# Метка запуска
print(f"Скрипт: list_project_structure.py, Версия: 1.0, Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, Ожидаемое время выполнения: ~1 минута")

# Функция для рекурсивного вывода структуры файлов и папок
def list_directory_structure(path, exclude_dir=".venv"):
    for root, dirs, files in os.walk(path):
        if exclude_dir in dirs:
            dirs.remove(exclude_dir)  # Исключение .venv
        print(f"Папка: {os.path.relpath(root, path)}")
        for file in files:
            print(f"  Файл: {os.path.join(os.path.relpath(root, path), file)}")

# Вывод структуры проекта
project_path = r"D:\PycharmProjects\ONNX_bot"
list_directory_structure(project_path)

# Вывод установленных библиотек и версий
print("\nУстановленные библиотеки и зависимости:")
installed_packages = pkg_resources.working_set
for package in sorted(["%s==%s" % (i.key, i.version) for i in installed_packages]):
    print(f"  {package}")

# Завершение
print(f"Скрипт: list_project_structure.py, Версия: 1.0, Завершение: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")