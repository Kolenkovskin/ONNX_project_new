import os
import pandas as pd
import random

# Настройка pandas для полного отображения
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.6f}'.format)
pd.set_option('display.max_columns', None)

# Папка с Renko-файлами
data_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"


# Функция для вывода строк из файла
def print_rows_from_file(file_path):
    filename = os.path.basename(file_path)
    print(f"\n=== Анализ файла: {filename} ===")

    try:
        # Загрузка файла
        df = pd.read_csv(file_path, low_memory=False, parse_dates=['Time (EET)', 'EndTime'])
        print(f"Форма DataFrame: {df.shape} (строк: {df.shape[0]}, столбцов: {df.shape[1]})")

        # Первые 5 строк
        print("\n--- Первые 5 строк ---")
        print(df.head(5).to_string(index=True))

        # Последние 5 строк
        print("\n--- Последние 5 строк ---")
        print(df.tail(5).to_string(index=True))

        # 15 случайных строк, исключая первые и последние 5
        if len(df) > 10:
            exclude_indices = list(range(5)) + list(range(len(df) - 5, len(df)))
            df_middle = df.drop(exclude_indices)
            if len(df_middle) >= 15:
                random_rows = df_middle.sample(n=15, random_state=random.randint(0, 10000))
                print("\n--- 15 случайных строк (исключая первые и последние 5) ---")
                print(random_rows.to_string(index=True))
            else:
                print("\n--- 15 случайных строк: Недостаточно строк после исключения ---")
        else:
            print("\n--- 15 случайных строк: Файл слишком мал ---")

    except Exception as e:
        print(f"Ошибка обработки {filename}: {str(e)}")


# Основной код
if __name__ == "__main__":
    # Список Renko-файлов
    renko_files = [
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.startswith("XAUUSD_Renko_ONE_PIP") and f.endswith(".csv")
    ]

    print(f"Найдено файлов: {len(renko_files)}")

    for file_path in renko_files:
        print_rows_from_file(file_path)

    print("\nОбработка завершена.")