# Метка: list_files_and_libraries_20250601_2344
# Дата и время запуска: 01 июня 2025, 23:44 EEST

import os
import pkg_resources
from datetime import datetime

print(f"Скрипт запущен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Папки для анализа
dirs = [
    r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_other",
    r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_processed",
    r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
]

# 1. Список файлов в папках
print("\nСписок файлов в указанных папках:")
for dir_path in dirs:
    print(f"\nПапка: {dir_path}")
    if os.path.exists(dir_path):
        files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
        if files:
            for file in files:
                print(f"  - {file}")
        else:
            print("  Папка пуста или содержит только подпапки")
    else:
        print("  Папка не существует")

# 2. Список установленных библиотек
print("\nСписок установленных библиотек в проекте:")
installed_packages = pkg_resources.working_set
for package in sorted(installed_packages, key=lambda x: x.key):
    print(f"  - {package.key}=={package.version}")

print("\nПроверка завершена.")