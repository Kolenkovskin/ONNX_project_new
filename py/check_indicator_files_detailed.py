import os
import pandas as pd
import numpy as np
import random
import datetime
import talib  # Для перепроверки индикаторов; установите через pip если нет
from multiprocessing import Pool, cpu_count  # Для параллельной обработки

# Путь к папке с данными
data_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"

# Путь к папке для вывода
output_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\txt"
os.makedirs(output_dir, exist_ok=True)


# Функция для анализа одного файла (для параллелизации)
def analyze_file(file_path):
    filename = os.path.basename(file_path)
    output_filename = os.path.join(output_dir,
                                   f"analysis_{filename.replace('.csv', '')}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")
    output = []

    output.append(f"=== Анализ файла: {filename} ===")
    output.append(f"Время начала анализа: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Загрузка файла с оптимизацией для больших файлов
        df = pd.read_csv(file_path, low_memory=False,
                         parse_dates=['Time (EET)', 'EndTime'] if 'Time (EET)' in pd.read_csv(file_path,
                                                                                              nrows=1).columns else None)
        output.append(f"Форма DataFrame: {df.shape} (строк: {df.shape[0]}, столбцов: {df.shape[1]})")

        # Общая информация
        output.append("\n--- Общая информация ---")
        info_str = str(df.info())
        output.append(info_str)

        # Описательная статистика
        output.append("\n--- Описательная статистика ---")
        output.append(str(df.describe()))

        # Пропуски
        output.append("\n--- Пропуски (NaN) ---")
        nulls = df.isnull().sum()
        output.append(str(nulls[nulls > 0]))

        # Дубликаты
        output.append("\n--- Дубликаты ---")
        duplicates = df.duplicated().sum()
        output.append(f"Количество дубликатов: {duplicates}")

        # Последовательность времени
        if 'Time (EET)' in df.columns:
            df_sorted = df.sort_values('Time (EET)')
            is_monotonic = df_sorted['Time (EET)'].is_monotonic_increasing
            output.append(f"Временная последовательность monotonic_increasing: {is_monotonic}")
            time_gaps = df_sorted['Time (EET)'].diff().dropna()
            max_gap = time_gaps.max()
            min_gap = time_gaps.min()
            output.append(f"Макс. разрыв времени: {max_gap}, Мин. разрыв: {min_gap}")

        # Аномалии
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if 'Close' in col or 'Open' in col or 'High' in col or 'Low' in col:
                anomalies_neg = (df[col] < 0).sum()
                anomalies_extreme = (df[col] > 10000).sum()
                output.append(
                    f"Аномалии в {col}: Отрицательные: {anomalies_neg}, Экстремальные (>10000): {anomalies_extreme}")
            if 'Volume' in col:
                anomalies_neg_vol = (df[col] < 0).sum()
                output.append(f"Аномалии в {col}: Отрицательные: {anomalies_neg_vol}")
            if 'Spread' in col:
                anomalies_spread = (df[col] > 10).sum()
                output.append(f"Аномалии в Spread: >10 пунктов: {anomalies_spread}")

        # Проверка индикаторов (перерасчет на выборке, если файл большой)
        sample_size = min(1000, len(df))  # Для больших файлов проверяем на выборке
        df_sample = df.sample(n=sample_size, random_state=42)

        if all(col in df.columns for col in ['Close', 'EMA_20']):
            recalculated_ema20 = talib.EMA(df_sample['Close'], timeperiod=20)
            ema_match = np.allclose(df_sample['EMA_20'].dropna(), recalculated_ema20.dropna(), atol=1e-5)
            output.append(f"Проверка EMA_20 (на выборке): Совпадает: {ema_match}")
        if 'RSI_14' in df.columns:
            recalculated_rsi = talib.RSI(df_sample['Close'], timeperiod=14)
            rsi_match = np.allclose(df_sample['RSI_14'].dropna(), recalculated_rsi.dropna(), atol=1e-5)
            output.append(
                f"Проверка RSI_14 (на выборке): Совпадает: {rsi_match}, Аномалии (не в 0-100): {(df_sample['RSI_14'] < 0).sum() + (df_sample['RSI_14'] > 100).sum()}")
        if 'ATR_14' in df.columns:
            recalculated_atr = talib.ATR(df_sample['High'], df_sample['Low'], df_sample['Close'], timeperiod=14)
            atr_match = np.allclose(df_sample['ATR_14'].dropna(), recalculated_atr.dropna(), atol=1e-5)
            output.append(f"Проверка ATR_14 (на выборке): Совпадает: {atr_match}")
        if 'VWAP' in df.columns and 'Volume' in df.columns:
            recalculated_vwap = (df_sample['Close'] * df_sample['Volume']).cumsum() / df_sample['Volume'].cumsum()
            vwap_match = np.allclose(df_sample['VWAP'].dropna(), recalculated_vwap.dropna(), atol=1e-5)
            output.append(f"Проверка VWAP (на выборке): Совпадает: {vwap_match}")
        if 'Fib_0_0' in df.columns:
            recalculated_fib00 = df_sample['Low'].rolling(window=20).min()
            fib_match = np.allclose(df_sample['Fib_0_0'].dropna(), recalculated_fib00.dropna(), atol=1e-5)
            output.append(f"Проверка Fib_0_0 (на выборке): Совпадает: {fib_match}")
        if 'CDL_DOJI' in df.columns:
            recalculated_doji = talib.CDLDOJI(df_sample['Open'], df_sample['High'], df_sample['Low'],
                                              df_sample['Close'])
            doji_match = np.allclose(df_sample['CDL_DOJI'].dropna(), recalculated_doji.dropna(), atol=1e-5)
            output.append(f"Проверка CDL_DOJI (на выборке): Совпадает: {doji_match}")

        # Ложные пробои
        if all(col in df.columns for col in ['High', 'Low', 'Close', 'Open']):
            false_breakouts = ((df['High'] > df['High'].shift(1)) & (df['Close'] < df['Open'])).sum()
            output.append(f"Потенциальные ложные пробои: {false_breakouts}")

        # Вывод строк
        output.append("\n--- Первые 5 строк ---")
        output.append(str(df.head(5)))

        output.append("\n--- Последние 5 строк ---")
        output.append(str(df.tail(5)))

        # 15 случайных строк, исключая первые и последние 5
        if len(df) > 10:
            exclude_indices = list(range(5)) + list(range(len(df) - 5, len(df)))
            df_middle = df.drop(exclude_indices)
            random_rows = df_middle.sample(n=min(15, len(df_middle)),
                                           random_state=random.randint(0, 10000))  # Random seed для уникальности
            output.append("\n--- 15 случайных строк (из середины) ---")
            output.append(str(random_rows))
        else:
            output.append("\n--- 15 случайных строк: Файл слишком мал ---")

    except Exception as e:
        output.append(f"Ошибка анализа: {str(e)}")

    output.append(f"Время окончания анализа: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Сохранение в отдельный файл
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write("\n".join(output))

    # Подсчет строк и добавление в конец
    with open(output_filename, 'r', encoding='utf-8') as f:
        line_count = sum(1 for _ in f)
    with open(output_filename, 'a', encoding='utf-8') as f:
        f.write(f"\n\n=== Итог: Количество строк в этом файле: {line_count} ===")

    return f"Анализ {filename} сохранен в {output_filename}"


# Основной код
if __name__ == "__main__":
    # Список CSV файлов
    csv_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    print(f"Найдено файлов: {len(csv_files)}")

    # Параллельная обработка
    with Pool(processes=cpu_count() // 2) as pool:
        results = pool.map(analyze_file, csv_files)

    # Вывод результатов
    for result in results:
        print(result)

    print("Анализ всех файлов завершен.")