# prepare_data.py
# Уникальная метка: PREPARE_DATA_V11
# Дата и время запуска: 2025-05-30 07:12:00 EEST

import pandas as pd
import os
from datetime import datetime
import ta


class DataProcessingError(Exception):
    """Исключение для ошибок обработки данных."""
    pass


def load_and_clean_csv(file_path, file_type='standard'):
    try:
        # Загрузка CSV с правильным разделителем
        df = pd.read_csv(file_path, sep=';', decimal=',', thousands=None, on_bad_lines='warn')
    except Exception as e:
        raise DataProcessingError(f"Ошибка при чтении {file_path}: {e}")

    # Проверка и переименование столбцов
    if file_type == 'standard':
        if 'Close' not in df.columns:
            if 'Ask' in df.columns:
                df['Close'] = df['Ask']
            elif 'Bid' in df.columns:
                df['Close'] = df['Bid']
            else:
                raise DataProcessingError(f"В файле {file_path} отсутствуют столбцы 'Close', 'Ask' или 'Bid'.")
    elif file_type == 'ticks':
        if 'Ask' in df.columns and 'Bid' in df.columns:
            df['Close'] = (df['Ask'] + df['Bid']) / 2  # Среднее между Ask и Bid
        else:
            raise DataProcessingError(f"В файле {file_path} отсутствуют столбцы 'Ask' или 'Bid' для тиков.")

    # Преобразование столбцов с ценами в float
    price_columns = ['Open', 'High', 'Low', 'Close']
    for col in price_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Преобразование столбца времени
    if 'Time (EET)' in df.columns:
        df['Time (EET)'] = pd.to_datetime(df['Time (EET)'], errors='coerce')

    return df


def add_indicators(df, file_type='standard'):
    # Проверка наличия необходимых столбцов
    if 'Close' not in df.columns:
        raise DataProcessingError("Столбец 'Close' отсутствует после обработки. Индикаторы не могут быть добавлены.")

    # Добавление индикаторов
    try:
        df['RSI'] = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
        df['MACD'] = ta.trend.MACD(df['Close']).macd()
        if file_type != 'ticks' and all(col in df.columns for col in ['High', 'Low', 'Close']):
            df['BB_High'] = ta.volatility.BollingerBands(df['Close']).bollinger_hband()
            df['BB_Low'] = ta.volatility.BollingerBands(df['Close']).bollinger_lband()
            df['ATR'] = ta.volatility.AverageTrueRange(high=df['High'], low=df['Low'],
                                                       close=df['Close']).average_true_range()
            df['Stochastic'] = ta.momentum.StochasticOscillator(high=df['High'], low=df['Low'],
                                                                close=df['Close']).stoch()
        print(f"Индикаторы успешно добавлены для файла типа {file_type}.")
    except Exception as e:
        raise DataProcessingError(f"Ошибка при добавлении индикаторов: {e}")

    return df


def process_file(file_path, output_dir, file_type='standard'):
    print(f"Обработка файла: {file_path} (тип: {file_type})")
    df = load_and_clean_csv(file_path, file_type)

    if df is None or df.empty:
        raise DataProcessingError(f"Файл {file_path} пуст или содержит ошибки данных.")

    # Добавление индикаторов
    df = add_indicators(df, file_type)

    # Сохранение обработанного файла
    file_name = os.path.basename(file_path).replace('.csv', '_processed.csv')
    output_path = os.path.join(output_dir, file_name)
    df.to_csv(output_path, index=False)
    print(f"Сохранён файл: {output_path}")


def process_files(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    for file in os.listdir(input_dir):
        if file.endswith('.csv'):
            if 'Renko' in file:
                print(f"Предупреждение: Пропуск файла {file}, так как Renko файлы обрабатываются отдельно.")
                continue
            file_path = os.path.join(input_dir, file)
            file_type = 'ticks' if 'Ticks' in file else 'standard'
            process_file(file_path, output_dir, file_type)


if __name__ == "__main__":
    try:
        input_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
        output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
        process_files(input_dir, output_dir)
    except DataProcessingError as e:
        print(f"Критическая ошибка: {e}")
        exit(1)