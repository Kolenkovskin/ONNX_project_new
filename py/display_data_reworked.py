# display_data_reworked.py
# Уникальная метка: DISPLAY_DATA_REWORKED_V1
# Дата и время запуска: 2025-06-01 11:46:00 EEST

import os

def display_file_content(file_path, num_lines=6):
    print(f"\n=== Первые {num_lines} строк файла: {file_path} (как в Блокноте) ===")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if i > num_lines:
                    break
                print(line.rstrip())
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")

def display_data_reworked(directory):
    print(f"\nСодержимое папки: {directory}")
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        if os.path.isfile(file_path):
            display_file_content(file_path)

if __name__ == "__main__":
    directory = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
    display_data_reworked(directory)