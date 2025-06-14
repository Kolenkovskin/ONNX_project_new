# Метка: check_original_suitability_20250525_2250
# Запуск: 2025-05-25 22:50 EEST

import os
import csv
from datetime import datetime, timedelta
import pandas as pd

# Путь к папке original
original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"

# Ожидаемые столбцы для большинства файлов (кроме Renko и Ticks)
expected_cols = ["Time (EET)", "Open", "High", "Low", "Close", "Volume"]

# Получаем список CSV файлов
files = [f for f in os.listdir(original_dir) if f.endswith(".csv")]

for file in files:
    file_path = os.path.join(original_dir, file)
    print(f"\nПроверка файла: {file}")

    try:
        # 1. Проверка на повреждения (построчное чтение)
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Заголовок
            expected_fields = len(header)

            line_count = 0
            first_date = None
            last_date = None
            missing_dates = 0

            for i, row in enumerate(reader, 2):
                line_count += 1

                # Проверка на неполные строки
                if len(row) != expected_fields:
                    print(f"Неполная строка {i}: {row}, ожидалось {expected_fields} полей, найдено {len(row)}")

                # Проверка на нечитаемые символы
                if any('\ufffd' in field for field in row):
                    print(f"Обнаружены нечитаемые символы в строке {i}: {row}")

                # Проверка диапазона дат
                try:
                    date_str = row[0]
                    current_date = datetime.strptime(date_str.split('.')[0], '%Y.%m.%d %H:%M:%S')
                    if first_date is None:
                        first_date = current_date
                    else:
                        # Проверка пропусков в датах (для 1m, 5m, 15m, 30m, 1h, 4h, daily)
                        if "Min" in file or "Hourly" in file:
                            time_diff = (current_date - last_date).total_seconds() / 60
                            expected_diff = {"1 Min": 1, "5 Mins": 5, "15 Mins": 15, "30 Mins": 30, "Hourly": 60,
                                             "4 Hours": 240}
                            for key, diff in expected_diff.items():
                                if key in file:
                                    if time_diff > diff:
                                        missing_dates += int(time_diff // diff - 1)
                                        print(f"Пропуск данных между {last_date} и {current_date}: {time_diff} минут")
                                    break
                        elif "Daily" in file:
                            time_diff = (current_date - last_date).days
                            if time_diff > 1:
                                missing_dates += time_diff - 1
                                print(f"Пропуск данных между {last_date} и {current_date}: {time_diff} дней")
                    last_date = current_date
                except (ValueError, IndexError):
                    print(f"Некорректная дата в строке {i}: {row}")

            # Статистика по строкам и датам
            print(f"Всего строк (без заголовка): {line_count}")
            if first_date and last_date:
                print(f"Диапазон дат: {first_date} - {last_date}")
                expected_minutes = (last_date - first_date).total_seconds() / 60 + 1
                print(f"Ожидаемое количество минут: {int(expected_minutes)}")
                print(f"Пропущено интервалов: {missing_dates}")
                print(f"Процент заполненности: {(line_count / (line_count + missing_dates)) * 100:.2f}%")

        # 2. Проверка через pandas (ошибки формата, пропуски, корректность значений)
        try:
            # Читаем файл как текст для исправления формата чисел
            lines = []
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            fixed_lines = []
            for line in lines:
                parts = line.strip().split(',')
                if parts[0].startswith("Time (EET)"):
                    parts = [part if part != "Volume " else "Volume" for part in parts]
                    fixed_lines.append(','.join(parts) + '\n')
                    continue
                fixed_parts = []
                for i, part in enumerate(parts):
                    if i < 2 and "Time" in part:
                        fixed_parts.append(part)
                    else:
                        if ',' in part and part.replace(',', '').replace('.', '').replace('-', '').isdigit():
                            fixed_parts.append(part.replace(',', '.'))
                        else:
                            fixed_parts.append(part)
                fixed_lines.append(','.join(fixed_parts) + '\n')

            # Сохраняем временный файл для проверки
            temp_file = file_path.replace('.csv', '_temp.csv')
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.writelines(fixed_lines)

            # Читаем через pandas
            df = pd.read_csv(temp_file, sep=',', encoding='utf-8')

            # Проверка заголовков
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols and "Renko" not in file and "Ticks" not in file:
                print(f"Отсутствуют столбцы: {missing_cols}")

            # Проверка пропусков (NaN)
            nan_counts = df.isna().sum()
            for col, count in nan_counts.items():
                if count > 0:
                    print(f"Пропуски в столбце {col}: {count} ({count / len(df) * 100:.2f}%)")

            # Проверка корректности значений (для цен XAUUSD)
            price_cols = ["Open", "High", "Low", "Close"]
            for col in price_cols:
                if col in df.columns:
                    min_val = df[col].min()
                    max_val = df[col].max()
                    if min_val < 1000 or max_val > 3000:  # Ожидаемый диапазон для XAUUSD
                        print(f"Аномальные значения в {col}: min={min_val}, max={max_val}")

            # Удаляем временный файл
            os.remove(temp_file)

        except Exception as e:
            print(f"Ошибка при анализе через pandas: {e}")

    except Exception as e:
        print(f"Общая ошибка при проверке файла: {e}")

print("Проверка пригодности завершена.")