# Метка: ListFoldersContentsAndLibs_20250530_0508
# Дата и время запуска: 30 мая 2025, 05:08 EEST
import os
import subprocess
from datetime import datetime

# Путь к директории проекта
project_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot"

# Папки, которые нужно исключить
exclude_dirs = {'.git', '.idea', '.venv'}

print(f"[{datetime.now()}] Список содержимого всех папок и подпапок в {project_dir} (кроме {exclude_dirs})")

# Проверка существования директории
if not os.path.exists(project_dir):
    raise Exception(f"Директория {project_dir} не найдена")


# Рекурсивное сканирование всех папок и подпапок, исключая указанные
def list_all_contents(directory):
    contents = []
    for root, dirs, files in os.walk(directory):
        # Исключаем указанные папки
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        # Добавляем путь к текущей папке
        if os.path.basename(root) not in exclude_dirs:
            contents.append(f"Папка: {root}")
            # Добавляем все файлы в текущей папке
            for file_name in files:
                full_path = os.path.join(root, file_name)
                contents.append(f"  Файл: {full_path}")
    return contents


# Получение списка содержимого
all_contents = list_all_contents(project_dir)

# Вывод списка содержимого
if all_contents:
    print("Найдено следующее содержимое:")
    for item in all_contents:
        print(item)
else:
    print("Содержимое не найдено")

# Получение списка установленных библиотек
print("\nПолучение списка установленных библиотек...")
try:
    # Используем команду pip list для получения списка библиотек
    result = subprocess.run(
        [r"C:\Users\Estal\PycharmProjects\ONNX_bot\.venv\Scripts\pip.exe", "list"],
        capture_output=True,
        text=True,
        check=True
    )
    libraries = result.stdout.splitlines()

    print("Установленные библиотеки:")
    for lib in libraries[2:]:  # Пропускаем первые две строки (заголовок таблицы)
        if lib:  # Пропускаем пустые строки
            print(f" - {lib}")
except subprocess.CalledProcessError as e:
    print(f"Ошибка при получении списка библиотек: {e}")
except FileNotFoundError:
    print("Не удалось найти pip.exe. Убедитесь, что виртуальное окружение настроено корректно.")

print("\nСканирование завершено")