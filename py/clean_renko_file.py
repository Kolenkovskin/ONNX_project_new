# clean_renko_file.py
# Уникальная метка: CLEAN_RENKO_618
# Дата и время выполнения: 2025-05-30 06:56:00 EEST
import csv
import os


def clean_csv_file(input_path, output_path, expected_fields=6):
    print(f"Очистка файла: {input_path}")
    valid_rows = []
    skipped_count = 0
    max_log_skipped = 100  # Логировать только первые 100 пропущенных строк

    # Создаём временный файл для промежуточного сохранения
    temp_output = output_path + '.tmp'

    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Сохраняем заголовок
        valid_rows.append(header)

        with open(temp_output, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)  # Записываем заголовок

            for line_num, row in enumerate(reader, start=2):
                if len(row) == expected_fields:
                    writer.writerow(row)  # Сразу записываем в файл
                else:
                    skipped_count += 1
                    if skipped_count <= max_log_skipped:
                        print(f"Пропущена строка {line_num}: ожидалось {expected_fields} полей, найдено {len(row)}")

                # Промежуточное сохранение каждые 100,000 строк
                if line_num % 100_000 == 0:
                    print(f"Обработано {line_num} строк, пропущено {skipped_count} строк")

    # Переименовываем временный файл в итоговый
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.replace(temp_output, output_path)

    print(f"Очищенный файл сохранён: {output_path}")
    print(f"Всего пропущено строк: {skipped_count}")


if __name__ == "__main__":
    input_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv"
    output_file = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25_cleaned.csv"
    clean_csv_file(input_file, output_file)