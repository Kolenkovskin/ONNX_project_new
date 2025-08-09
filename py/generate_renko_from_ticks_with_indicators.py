import pandas as pd
import os
import sys
import time
import numpy as np
import talib
import psutil
from multiprocessing import Pool, cpu_count
import datetime


def generate_renko_from_chunk(chunk, price_col, volume_col):
    """Генерирует Renko-данные для заданного столбца цены и объема."""
    renko_data = []
    if chunk.empty or price_col not in chunk.columns or volume_col not in chunk.columns:
        return pd.DataFrame(renko_data, columns=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'])

    current_brick = chunk[price_col].iloc[0]
    brick_start_time = chunk['Time (EET)'].iloc[0]
    volume_accum = 0.0

    for i in range(len(chunk)):
        price = chunk[price_col].iloc[i]
        time_current = chunk['Time (EET)'].iloc[i]
        volume_accum += chunk[volume_col].iloc[i]

        diff = price - current_brick
        if abs(diff) >= 0.1:  # Размер кирпича Renko: 1 пункт (0.1)
            num_bricks = int(abs(diff) / 0.1)
            brick_direction = np.sign(diff)

            for _ in range(num_bricks):
                open_price = current_brick
                close_price = current_brick + brick_direction * 0.1
                high_price = max(open_price, close_price)
                low_price = min(open_price, close_price)

                renko_data.append(
                    [brick_start_time, time_current, open_price, high_price, low_price, close_price, volume_accum])
                current_brick = close_price
                brick_start_time = time_current
                volume_accum = 0.0

    return pd.DataFrame(renko_data, columns=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'])


if __name__ == '__main__':
    UNIQUE_MARKER = f"UNIQUE_MARKER: generate_renko_fixed_2025-08-08_21-01-00"
    print(UNIQUE_MARKER)
    start_time = time.time()
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Скрипт generate_renko_fixed запущен")

    # Проверка памяти
    memory = psutil.virtual_memory()
    print(f"Доступная память: {memory.available / (1024 ** 3):.2f} GB")

    # Путь к тиковому файлу
    file_path = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_cleaned\XAUUSD_Ticks_2020.01.01_2025.05.25_cleaned.csv"

    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        sys.exit(1)

    # Папка для вывода
    output_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"
    os.makedirs(output_dir, exist_ok=True)
    output_path_ask = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Ask_With_Indicators_2020_2025.csv")
    output_path_bid = os.path.join(output_dir, "XAUUSD_Renko_ONE_PIP_Bid_With_Indicators_2020_2025.csv")

    chunk_size = 2500000  # Размер чанка
    total_rows = sum(1 for _ in open(file_path, encoding='utf-8')) - 1  # Минус заголовок
    processed_rows = 0

    try:
        # Загрузка тиковых данных по чанкам
        chunks = pd.read_csv(file_path, sep=';', chunksize=chunk_size, encoding='utf-8',
                             parse_dates=['Time (EET)'], low_memory=False)
        renko_df_ask = pd.DataFrame(columns=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
        renko_df_bid = pd.DataFrame(columns=['Time (EET)', 'EndTime', 'Open', 'High', 'Low', 'Close', 'Volume'])

        with Pool(cpu_count() // 2) as p:  # Половина ядер (8 из 16)
            for chunk in chunks:
                processed_rows += len(chunk)
                print(f"Обработка чанка: {len(chunk)} строк (всего: {processed_rows}/{total_rows})")

                # Проверка аномалий
                for col in ['Ask', 'Bid', 'AskVolume', 'BidVolume']:
                    if col in chunk.columns:
                        if (chunk[col] < 0).any() or (chunk[col] > 10000).any():
                            print(f"Аномалии в {col}: Отрицательные или экстремальные значения")

                # Сортировка по времени
                if not chunk['Time (EET)'].is_monotonic_increasing:
                    chunk = chunk.sort_values(by='Time (EET)').reset_index(drop=True)

                # Параллельная обработка
                results = p.starmap(generate_renko_from_chunk, [
                    (chunk, 'Ask', 'AskVolume'),
                    (chunk, 'Bid', 'BidVolume')
                ])
                renko_df_ask = pd.concat([renko_df_ask, results[0]], ignore_index=True, copy=False)
                renko_df_bid = pd.concat([renko_df_bid, results[1]], ignore_index=True, copy=False)

        print(f"Сгенерировано Renko строк для Ask: {len(renko_df_ask)}")
        print(f"Сгенерировано Renko строк для Bid: {len(renko_df_bid)}")

        # Добавление индикаторов
        for df, output_path in [(renko_df_ask, output_path_ask), (renko_df_bid, output_path_bid)]:
            if len(df) > 50:
                df['EMA_20'] = talib.EMA(df['Close'], timeperiod=20)
                df['EMA_50'] = talib.EMA(df['Close'], timeperiod=50)
                df['RSI_14'] = talib.RSI(df['Close'], timeperiod=14)
                df['ATR_14'] = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
                if df['Volume'].sum() > 0:
                    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
                else:
                    df['VWAP'] = df['Close']
                    print(f"Предупреждение: Нулевые объемы в {output_path}, VWAP = Close")
                period = 20
                df['Fib_0_0'] = df['Low'].rolling(window=period).min()
                df['Fib_100_0'] = df['High'].rolling(window=period).max()
                df['Fib_23_6'] = df['Fib_0_0'] + 0.236 * (df['Fib_100_0'] - df['Fib_0_0'])
                df['Fib_38_2'] = df['Fib_0_0'] + 0.382 * (df['Fib_100_0'] - df['Fib_0_0'])
                df['Fib_50_0'] = df['Fib_0_0'] + 0.5 * (df['Fib_100_0'] - df['Fib_0_0'])
                df['Fib_61_8'] = df['Fib_0_0'] + 0.618 * (df['Fib_100_0'] - df['Fib_0_0'])
                df['CDL_DOJI'] = talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close'])
                df['CDL_HAMMER'] = talib.CDLHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
                df['CDL_ENGULFING'] = talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close'])
                print(f"Индикаторы добавлены для {output_path}")
            else:
                print(f"Недостаточно данных для индикаторов в {output_path}")

            # Сохранение
            df.to_csv(output_path, index=False)
            print(f"Renko с индикаторами сохранен в: {output_path}")

            # Проверка
            saved_df = pd.read_csv(output_path, low_memory=False)
            print(f"Проверка: {len(saved_df)} строк")
            print(f"Пропуски: {saved_df.isnull().sum().to_dict()}")
            print(f"Дубликаты: {saved_df.duplicated().sum()}")

    except Exception as e:
        print(f"Ошибка обработки: {str(e)}")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Завершено. Время: {total_time:.2f} секунд")