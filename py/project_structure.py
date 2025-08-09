import os
import datetime

# Уникальная метка для этой версии скрипта
MARKER = f"xAI_Marker_v1_ONNX_bot_structure_{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

print(f"Запуск скрипта: {MARKER}")

# Путь к корневой папке проекта
root_dir = r"C:\Users\User\PycharmProjects\ONNX_bot"

# Путь к папке для сохранения файла
output_dir = os.path.join(root_dir, "txt")
os.makedirs(output_dir, exist_ok=True)  # Создаем папку, если не существует

# Путь к выходному файлу
output_file = os.path.join(output_dir, "project_structure.txt")


# Функция для обхода директорий
def traverse_directory(root):
    output = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Исключаем папку .venv
        if '.venv' in dirpath:
            continue

        # Добавляем текущую папку
        rel_path = os.path.relpath(dirpath, root)
        if rel_path == '.':
            rel_path = ''
        output.append(f"Папка: {rel_path}")

        # Добавляем подпапки
        for dirname in dirnames:
            if dirname == '.venv':
                continue  # Пропускаем .venv
            output.append(f"  Подпапка: {dirname}")

        # Добавляем файлы
        for filename in filenames:
            output.append(f"  Файл: {filename}")
            file_path = os.path.join(dirpath, filename)

            # Если файл .py, добавляем его содержимое
            if filename.endswith('.py'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    output.append("    Содержимое:")
                    output.append(content)
                    output.append("    --- Конец содержимого ---")
                except Exception as e:
                    output.append(f"    Ошибка чтения содержимого: {e}")

    return output


# Получаем вывод
structure_output = traverse_directory(root_dir)

# Выводим в консоль
for line in structure_output:
    print(line)

# Сохраняем в файл
try:
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Запуск скрипта: {MARKER}\n\n")
        for line in structure_output:
            f.write(line + '\n')
    print(f"\nВывод сохранен в файл: {output_file}")
except Exception as e:
    print(f"Ошибка сохранения файла: {e}")