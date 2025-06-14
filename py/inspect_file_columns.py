# inspect_file_columns.py
# Уникальная метка: INSPECT_FILE_COLUMNS_V3
# Дата и время запуска: 2025-05-30 06:49:00 EEST

import pandas as pd
import os

def inspect_file_columns(file_path):
    try:
        # Читаем файл для получения столбцов
        df = pd.read_csv(file_path, sep=';', decimal=',', thousands=None, nrows=5)
        print(f"\nФайл: {file_path}")
        print("Столбцы:", list(df.columns))
    except Exception as e:
        print(f"Ошибка при чтении столбцов {file_path}: {e}")

def display_raw_lines(file_path, num_lines=5):
    print(f"\n=== Первые {num_lines} строк файла: {file_path} (как в Блокноте) ===")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if i > num_lines:
                    break
                print(line.rstrip())
    except Exception as e:
        print(f"Ошибка при чтении строк {file_path}: {e}")

def inspect_all_files(directory):
    print(f"\n=== Проверка всех файлов в папке: {directory} ===")
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            file_path = os.path.join(directory, file)
            inspect_file_columns(file_path)
            display_raw_lines(file_path)

if __name__ == "__main__":
    original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
    inspect_all_files(original_dir)