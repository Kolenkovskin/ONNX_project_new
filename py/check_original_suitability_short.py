# Метка: check_original_suitability_short_20250525_2254
# Запуск: 2025-05-25 22:54 EEST

import os
import csv
from datetime import datetime
import pandas as pd

# Путь к папке original
original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"

# Получаем список CSV файлов
files = [f for f in os.listdir(original_dir) if f.endswith(".csv")]

for file in files:
    file_path = os.path.join(original_dir, file)
    print(f"\nПроверка файла: {file}")

    try:
        # 1. Проверка повреждений (первые и последние 1000 строк)
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            header = lines[0].strip().split(',')
            expected_fields = len(header)

            # Первые 1000 строк
            for i, line in enumerate(lines[1:1001], 2):
                fields = line.strip().split(',')
                if len(fields) != expected_fields:
                    print(f"Неполная строка {i}: ожидалось {expected_fields} полей, найдено {len(fields)}")
                if '\ufffd' in line:
                    print(f"Нечитаемые символы в строке {i}")

            # Последние 1000 строк
            for i, line in enumerate(lines[-1000:] if len(lines) > 1000 else lines[1:], len(lines) - 1000 + 2):
                fields = line.strip().split(',')
                if len(fields) != expected_fields:
                    print(f"Неполная строка {i}: ожидалось {expected_fields} полей, найдено {len(fields)}")
                if '\ufffd' in line:
                    print(f"Нечитаемые символы в строке {i}")

            # Проверка диапазона дат
            first_date = datetime.strptime(lines[1].split(',')[0].split('.')[0], '%Y.%m.%d %H:%M:%S')
            last_date = datetime.strptime(lines[-1].split(',')[0].split('.')[0], '%Y.%m.%d %H:%M:%S')
            print(f"Диапазон дат: {first_date} - {last_date}")

        # 2. Проверка через pandas (выборочно)
        temp_file = file_path.replace('.csv', '_temp.csv')
        with open(temp_file, 'w', encoding='utf-8') as f:
            fixed_lines = [lines[0].replace('Volume ', 'Volume')]
            for line in lines[1:]:
                parts = line.strip().split(',')
                fixed_parts = [parts[0]] + [
                    part.replace(',', '.') if ',' in part and part.replace(',', '').replace('.', '').isdigit() else part
                    for part in parts[1:]]
                fixed_lines.append(','.join(fixed_parts) + '\n')
            f.writelines(fixed_lines)

        df = pd.read_csv(temp_file, sep=',', nrows=1000, encoding='utf-8')
        df_tail = pd.read_csv(temp_file, sep=',', skiprows=max(1, len(lines) - 1000), nrows=1000, encoding='utf-8')

        # Проверка пропусков
        nan_counts = df.isna().sum() + df_tail.isna().sum()
        for col, count in nan_counts.items():
            if count > 0:
                print(f"Пропуски в столбце {col}: {count}")

        # Проверка аномалий в ценах
        for col in ["Open", "High", "Low", "Close"]:
            if col in df.columns:
                min_val = min(df[col].min(), df_tail[col].min())
                max_val = max(df[col].max(), df_tail[col].max())
                if min_val < 1000 or max_val > 3000:
                    print(f"Аномальные значения в {col}: min={min_val}, max={max_val}")

        os.remove(temp_file)

    except Exception as e:
        print(f"Ошибка: {e}")

print("Проверка завершена.")