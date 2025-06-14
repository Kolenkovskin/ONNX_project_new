# display_split_files.py
# Уникальная метка: DISPLAY_SPLIT_FILES_V1
# Дата и время запуска: 2025-05-30 06:31:00 EEST

import os


def display_first_six_lines(directory):
    # Проверяем, существует ли папка
    if not os.path.exists(directory):
        print(f"Папка {directory} не найдена.")
        return

    # Проходим по всем файлам в папке
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            file_path = os.path.join(directory, file)
            print(f"\n=== Файл: {file} ===")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Читаем первые 6 строк
                    for i, line in enumerate(f, 1):
                        if i > 6:
                            break
                        # Выводим строку без обработки, как в Блокноте
                        print(line.rstrip())
            except Exception as e:
                print(f"Ошибка при чтении файла {file}: {e}")


if __name__ == "__main__":
    split_files_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\split_files"
    display_first_six_lines(split_files_dir)