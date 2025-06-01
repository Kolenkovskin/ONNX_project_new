# Метка: check_original_files_modified_20250525_2226
# Запуск: 2025-05-25 22:26 EEST

import pandas as pd
import os

# Путь к папке original
original_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"

# Получаем список CSV файлов
files = [f for f in os.listdir(original_dir) if f.endswith(".csv")]

for file in files:
    file_path = os.path.join(original_dir, file)
    print(f"\nПроверка файла: {file}")

    # Читаем первые 15 строк как текст (как в Блокноте)
    print("\nПервые 15 строк файла (как в Блокноте):")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [next(f).strip() for _ in range(15)]  # Читаем первые 15 строк
            for i, line in enumerate(lines, 1):
                print(f"Строка {i}: {line}")
    except Exception as e:
        print(f"Ошибка при чтении файла как текста: {e}")

    # Проверка структуры через pandas
    print(f"\nПроверка структуры через pandas:")
    try:
        for sep in [",", ";", "\t"]:
            print(f"\nПопытка с разделителем: {sep}")
            try:
                df = pd.read_csv(file_path, sep=sep, nrows=5, encoding="utf-8")
                print("Столбцы:", df.columns.tolist())
                print("Первые 5 строк:\n", df)

                if len(df.columns) == 1:
                    print("Предупреждение: Обнаружен только 1 столбец. Вероятно, неверный разделитель.")
                elif len(df.columns) >= 5:
                    print("Структура выглядит корректной (5+ столбцов).")

                expected_cols = ["Time (EET)", "Open", "High", "Low", "Close", "Volume"]
                missing_cols = [col for col in expected_cols if col not in df.columns]
                if missing_cols:
                    print(f"Отсутствуют столбцы: {missing_cols}")
                else:
                    print("Все ожидаемые столбцы найдены.")

                if "Volume " in df.columns:
                    print("Обнаружен столбец 'Volume ' с пробелом.")

                if sep == ",":
                    break

            except Exception as e:
                print(f"Ошибка при чтении с разделителем {sep}: {e}")

    except Exception as e:
        print(f"Общая ошибка при чтении файла: {e}")

print("Проверка всех файлов завершена.")