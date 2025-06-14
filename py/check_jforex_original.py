# Метка: check_jforex_original_20250525_2200
# Запуск: 2025-05-25 22:00 EEST

import pandas as pd
import os

# Путь к папке jforex и original
jforex_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex"
original_dir = os.path.join(jforex_dir, "original")

# Собираем CSV файлы из папки jforex
jforex_files = [os.path.join(jforex_dir, f) for f in os.listdir(jforex_dir) if f.endswith(".csv")]

# Собираем CSV файлы из папки original
original_files = [os.path.join(original_dir, f) for f in os.listdir(original_dir) if f.endswith(".csv")]

# Объединяем списки файлов
files = jforex_files + original_files

for file_path in files:
    print(f"\nПроверка файла: {file_path}")

    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        continue

    try:
        # Читаем первые 5 строк с разными разделителями
        for sep in [",", ";", "\t"]:
            print(f"\nПопытка с разделителем: {sep}")
            try:
                df = pd.read_csv(file_path, sep=sep, nrows=5, encoding="utf-8")
                print("Столбцы:", df.columns.tolist())
                print("Первые 5 строк:\n", df)

                # Проверяем количество столбцов
                if len(df.columns) == 1:
                    print("Предупреждение: Обнаружен только 1 столбец. Вероятно, неверный разделитель.")
                elif len(df.columns) >= 6:
                    print("Структура выглядит корректной (6+ столбцов).")

                # Проверяем наличие ключевых столбцов
                expected_cols = ["Time (EET)", "Open", "High", "Low", "Close", "Volume"]
                missing_cols = [col for col in expected_cols if col not in df.columns]
                if missing_cols:
                    print(f"Отсутствуют столбцы: {missing_cols}")
                else:
                    print("Все ожидаемые столбцы найдены.")
                break  # Прерываем цикл, если разделитель подошёл
            except Exception as e:
                print(f"Ошибка при чтении с разделителем {sep}: {e}")

    except Exception as e:
        print(f"Общая ошибка при чтении файла: {e}")

print("Проверка всех файлов завершена.")