# diagnose_renko_files.py
# Уникальная метка: DIAGNOSE_RENKO_FILES_V1
# Дата и время запуска: 2025-05-30 07:02:00 EEST

import csv
import os
from collections import Counter


def diagnose_file(file_path, expected_fields=9):
    print(f"\nДиагностика файла: {file_path}")
    field_counts = Counter()
    incorrect_samples = []
    max_samples = 10  # Максимум примеров некорректных строк
    total_lines = 0

    try:
        with open(file_path, 'r', encoding='utf-8') as infile:
            reader = csv.reader(infile, delimiter=';')
            header = next(reader)  # Сохраняем заголовок
            field_counts[len(header)] += 1
            total_lines += 1
            print(f"Заголовок: {header}")
            print(f"Количество полей в заголовке: {len(header)}")

            for line_num, row in enumerate(reader, start=2):
                total_lines += 1
                num_fields = len(row)
                field_counts[num_fields] += 1
                if num_fields != expected_fields and len(incorrect_samples) < max_samples:
                    incorrect_samples.append((line_num, row))

                if total_lines % 1_000_000 == 0:
                    print(f"Обработано {total_lines} строк")

        print(f"\nСтатистика по количеству полей:")
        for fields, count in field_counts.items():
            print(f"Строк с {fields} полями: {count} ({count / total_lines * 100:.2f}%)")

        if incorrect_samples:
            print(f"\nПримеры строк с некорректным количеством полей (максимум {max_samples}):")
            for line_num, row in incorrect_samples:
                print(f"Строка {line_num}: {row} ({len(row)} полей)")
        else:
            print("\nНекорректных строк не найдено.")

    except Exception as e:
        print(f"Ошибка при диагностике {file_path}: {e}")


if __name__ == "__main__":
    files = [
        r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv",
        r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv"
    ]

    for file_path in files:
        diagnose_file(file_path)