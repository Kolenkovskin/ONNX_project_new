# check_processed_files.py
# Уникальная метка: CHECK_PROCESSED_FILES_V4
# Дата и время запуска: 2025-05-30 19:08:00 EEST

import pandas as pd
import os


def check_file(file_path):
    print(f"\nПроверка файла: {file_path}")
    try:
        df = pd.read_csv(file_path, sep=',')
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return

    print(f"Количество строк: {len(df)}")
    print(f"Столбцы: {list(df.columns)}")

    # Ожидаемые числовые столбцы
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'RSI', 'MACD', 'BB_High', 'BB_Low', 'ATR', 'Stochastic']
    available_columns = [col for col in numeric_columns if col in df.columns]

    # Проверка на нечисловые значения
    for col in available_columns:
        non_numeric = df[col].apply(lambda x: not isinstance(x, (int, float)) and pd.notna(x)).sum()
        if non_numeric > 0:
            print(f"  Ошибка: В столбце '{col}' найдено {non_numeric} нечисловых значений")

    # Проверка на пропуски
    if available_columns:
        missing = df[available_columns].isna().sum()
        for col, count in missing.items():
            if count > 0:
                print(f"  Пропуски: В столбце '{col}' пропущено {count} значений ({count / len(df) * 100:.2f}%)")

    # Проверка диапазона RSI
    if 'RSI' in df.columns and pd.notna(df['RSI']).any():
        rsi_min, rsi_max = df['RSI'].min(), df['RSI'].max()
        if not (0 <= rsi_min <= 100 and 0 <= rsi_max <= 100):
            print(f"  Ошибка: RSI вне диапазона [0, 100] (min: {rsi_min}, max: {rsi_max})")

    print("Проверка завершена")


def check_processed_files(directory):
    ticks_file = 'XAUUSD_Ticks_2024.01.01_2025.05.25_processed.csv'
    for file in os.listdir(directory):
        if file.endswith('_processed.csv') and file != ticks_file:
            file_path = os.path.join(directory, file)
            check_file(file_path)


if __name__ == "__main__":
    directory = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
    check_processed_files(directory)