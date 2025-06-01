# Метка: TrimAllFiles_20250529_1833
# Дата и время запуска: 29 мая 2025, 18:33 EEST
import os
import shutil
import pandas as pd

# Папки
source_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
split_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\split_files"

# 1. Удаление всех файлов и папок в split_files
if os.path.exists(split_dir):
    shutil.rmtree(split_dir)
os.makedirs(split_dir)

# 2. Копирование и обрезка файлов
max_rows = 500  # Количество строк для файла ~50 КБ
for file_name in os.listdir(source_dir):
    if file_name.endswith(".csv"):
        # Чтение файла
        file_path = os.path.join(source_dir, file_name)
        df = pd.read_csv(file_path, sep=';')  # Учитываем разделитель из JForex

        # Обрезка до max_rows
        df_trimmed = df.head(max_rows)

        # Сохранение обрезанного файла
        output_file = os.path.join(split_dir, f"{file_name[:-4]}_trimmed.csv")
        df_trimmed.to_csv(output_file, index=False, sep=';')
        print(f"Создан файл: {output_file}, строк: {len(df_trimmed)}")

print("Обрезка завершена")