# check_ticks_data.py
# Скрипт запущен: 2025-06-02 19:10:00
import pandas as pd
import time
import os
from pathlib import Path
from datetime import datetime
import glob

# Параметры
INPUT_FILE = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\XAUUSD_Ticks_2020_2025.csv"
REPORT_FILE = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\ticks_data_check_report.txt"
LOG_DIR = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\logs"
CHUNKSIZE = 1_000_000
EXPECTED_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

# Создание директории для логов
os.makedirs(LOG_DIR, exist_ok=True)


def is_valid_time_format(time_str):
    """Проверяет, соответствует ли строка формату времени."""
    try:
        datetime.strptime(time_str, EXPECTED_TIME_FORMAT)
        return True
    except ValueError:
        return False


def get_time_format(time_str):
    """Возвращает формат времени для строки, если возможно."""
    try:
        datetime.strptime(time_str, EXPECTED_TIME_FORMAT)
        return EXPECTED_TIME_FORMAT
    except ValueError:
        formats = [
            "%Y-%m-%d %H:%M:%S",  # Без миллисекунд
            "%d-%m-%Y %H:%M:%S.%f",  # DD-MM-YYYY
            "%Y/%m/%d %H:%M:%S.%f",  # YYYY/MM/DD
            "%Y-%m-%d %H:%M:%S.%f%z"  # С часовым поясом
        ]
        for fmt in formats:
            try:
                datetime.strptime(time_str, fmt)
                return fmt
            except ValueError:
                pass
        return "Unknown"


def is_numeric(value):
    """Проверяет, является ли значение числовым."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def list_available_files():
    """Возвращает список CSV-файлов в папке jforex и temp."""
    jforex_files = glob.glob(r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\*.csv")
    temp_files = glob.glob(r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\temp\*.csv")
    return jforex_files, temp_files


def check_data():
    print("Проверка тиковых данных...")
    start_time = time.time()

    # Проверка существования файла
    if not os.path.exists(INPUT_FILE):
        jforex_files, temp_files = list_available_files()
        error_msg = [
            f"Файл {INPUT_FILE} не найден.",
            "Доступные файлы в C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex:",
            "\n".join(jforex_files) if jforex_files else "Нет файлов",
            "Доступные файлы в C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\temp:",
            "\n".join(temp_files) if temp_files else "Нет файлов"
        ]
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write("\n".join(error_msg))
        print("\n".join(error_msg))
        print(f"Проверка завершена. Общее время: {time.time() - start_time:.2f} секунд")
        return

    # Инициализация счетчиков и списков
    total_rows = 0
    header_rows = 0
    invalid_time_rows = []
    invalid_ask_rows = []
    invalid_bid_rows = []
    missing_time = 0
    missing_ask = 0
    missing_bid = 0
    time_formats = set()
    duplicates = 0

    try:
        # Чтение файла по чанкам
        chunk_iter = pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE, low_memory=False)

        for chunk_idx, chunk in enumerate(chunk_iter):
            print(f"Обработка чанка {chunk_idx + 1}...")
            chunk_start_time = time.time()

            # Подсчет строк
            total_rows += len(chunk)

            # Проверка заголовков
            header_mask = chunk['Time (EET)'].astype(str).str.contains(r"Time \(EET\)", na=False)
            header_rows += header_mask.sum()
            if header_mask.any():
                invalid_time_rows.extend(chunk[header_mask].index.tolist())

            # Проверка формата времени
            for idx, time_val in chunk['Time (EET)'].items():
                if pd.notna(time_val):
                    time_str = str(time_val)
                    if not is_valid_time_format(time_str):
                        invalid_time_rows.append(idx)
                        time_formats.add(get_time_format(time_str))
                    else:
                        time_formats.add(EXPECTED_TIME_FORMAT)

            # Проверка пропусков
            missing_time += chunk['Time (EET)'].isna().sum()
            missing_ask += chunk['Ask'].isna().sum()
            missing_bid += chunk['Bid'].isna().sum()

            # Проверка числовых значений Ask и Bid
            for idx, row in chunk.iterrows():
                if pd.notna(row['Ask']) and not is_numeric(row['Ask']):
                    invalid_ask_rows.append(idx)
                if pd.notna(row['Bid']) and not is_numeric(row['Bid']):
                    invalid_bid_rows.append(idx)

            # Проверка дубликатов
            duplicates += chunk.duplicated().sum()

            print(f"Чанк {chunk_idx + 1} обработан за {time.time() - chunk_start_time:.2f} секунд")

        # Формирование отчета
        jforex_files, temp_files = list_available_files()
        report = [
            f"Проверка тиковых данных: {INPUT_FILE}",
            f"Время выполнения: {time.time() - start_time:.2f} секунд",
            f"Общее количество строк: {total_rows}",
            f"Строк с заголовком 'Time (EET)': {header_rows}",
            f"Строк с некорректным форматом времени: {len(invalid_time_rows)}",
            f"Уникальные форматы времени: {list(time_formats)}",
            f"Пропуски в Time (EET): {missing_time}",
            f"Пропуски в Ask: {missing_ask}",
            f"Пропуски в Bid: {missing_bid}",
            f"Некорректные значения Ask: {len(invalid_ask_rows)}",
            f"Некорректные значения Bid: {len(invalid_bid_rows)}",
            f"Дубликаты строк: {duplicates}",
            f"Примеры некорректных строк времени (первые 10): {invalid_time_rows[:10]}",
            f"Примеры некорректных Ask (первые 10): {invalid_ask_rows[:10]}",
            f"Примеры некорректных Bid (первые 10): {invalid_bid_rows[:10]}",
            "Доступные файлы в C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex:",
            "\n".join(jforex_files) if jforex_files else "Нет файлов",
            "Доступные файлы в C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\temp:",
            "\n".join(temp_files) if temp_files else "Нет файлов"
        ]

        # Сохранение отчета
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write("\n".join(report))

        print("Отчет сохранен:", REPORT_FILE)
        print("\n".join(report))

        # Проверка целостности
        if header_rows == 0 and len(
                invalid_time_rows) == 0 and missing_time == 0 and missing_ask == 0 and missing_bid == 0 and len(
                invalid_ask_rows) == 0 and len(invalid_bid_rows) == 0 and duplicates == 0:
            print("Файл целый, коррекция не требуется.")
        else:
            print("Файл содержит проблемы, требуется коррекция.")

    except Exception as e:
        print(f"Ошибка проверки данных: {e}")

    print(f"Проверка завершена. Общее время: {time.time() - start_time:.2f} секунд")


def main():
    total_start_time = time.time()
    check_data()
    print(f"Скрипт завершен. Общее время: {time.time() - total_start_time:.2f} секунд")


if __name__ == "__main__":
    print(f"Скрипт запущен: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    main()