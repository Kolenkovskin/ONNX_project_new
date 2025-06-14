# Метка: CheckRenkoIntegrity_20250529_2013
# Дата и время запуска: 29 мая 2025, 20:13 EEST
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta

# Пути к папкам
source_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\original"
output_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot\csv\jforex\data_reworked"
print(f"[{datetime.now()}] Проверка целостности Renko файлов в папке {source_dir}")

# Создание выходной папки
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Проход по Renko файлам
for file_name in os.listdir(source_dir):
    if file_name.endswith(".csv") and "Renko" in file_name:
        # Копирование файла в data_reworked
        src_path = os.path.join(source_dir, file_name)
        dst_path = os.path.join(output_dir, file_name)
        shutil.copy(src_path, dst_path)

        print(f"\nОбработка файла: {file_name}")
        try:
            # Чтение скопированного файла
            df = pd.read_csv(dst_path, sep=';', dtype=str)

            # 1. Количество строк
            row_count = len(df)
            print(f"Количество строк: {row_count}")

            # 2. Проверка названий столбцов
            print(f"Столбцы в файле: {list(df.columns)}")

            # 3. Преобразование числовых столбцов
            wick_col = next((col for col in df.columns if 'WickPrice' in col), None)
            opp_wick_col = next((col for col in df.columns if 'OppositeWickPrice' in col), None)

            for col in ['Open', 'High', 'Low', 'Close', 'Volume', wick_col, opp_wick_col]:
                if col and col in df.columns:
                    try:
                        def safe_float(x):
                            if pd.isna(x) or x is None:
                                return x
                            if isinstance(x, str):
                                try:
                                    return float(x.replace(',', '.'))
                                except ValueError:
                                    return x
                            return x


                        df[col] = df[col].apply(safe_float)

                        # Диагностика проблемных строк
                        problematic = df[df[col].apply(lambda x: isinstance(x, str))]
                        if not problematic.empty:
                            print(f"Проблемные строки в {col}:\n", problematic[[col]].head(5))
                    except Exception as e:
                        print(f"Ошибка преобразования столбца {col}: {str(e)}")

            # 4. Проверка пропусков
            missing_values = df.isnull().sum()
            print(f"Пропуски в данных:\n{missing_values}")

            # 5. Проверка дубликатов
            duplicates = df.duplicated().sum()
            print(f"Количество дубликатов: {duplicates}")

            # 6. Проверка формата времени
            if "Time (EET)" in df.columns:
                try:
                    df["Time (EET)"] = pd.to_datetime(df["Time (EET)"])
                    print("Формат времени корректен")
                except Exception as e:
                    print(f"Ошибка формата времени: {str(e)}")

            # 7. Проверка монотонности времени
            if "Time (EET)" in df.columns:
                time_diff = df["Time (EET)"].diff().dropna()
                non_monotonic = time_diff[time_diff <= pd.Timedelta(0)]
                print(f"Нарушений монотонности времени: {len(non_monotonic)}")
                if len(non_monotonic) > 0:
                    print("Примеры нарушений:\n", non_monotonic.head())

            # 8. Проверка положительности цен и объемов
            price_columns = [col for col in df.columns if col in ["Open", "High", "Low", "Close"]]
            volume_columns = [col for col in df.columns if col in ["Volume"]]

            for col in price_columns:
                negative_prices = df[col][df[col].apply(lambda x: isinstance(x, (int, float)) and x <= 0)]
                print(f"Отрицательные/нулевые значения в {col}: {len(negative_prices)}")

            for col in volume_columns:
                negative_volumes = df[col][df[col].apply(lambda x: isinstance(x, (int, float)) and x < 0)]
                print(f"Отрицательные значения в {col}: {len(negative_volumes)}")

            # 9. Проверка WickPrice и OppositeWickPrice
            if wick_col and opp_wick_col:
                wick_missing = df[wick_col].isnull().sum()
                opp_wick_missing = df[opp_wick_col].isnull().sum()
                print(f"Пропуски в {wick_col}: {wick_missing}")
                print(f"Пропуски в {opp_wick_col}: {opp_wick_missing}")

                # Проверка отрицательных значений
                for col in [wick_col, opp_wick_col]:
                    negative_values = df[col][df[col].apply(lambda x: isinstance(x, (int, float)) and x <= 0)]
                    print(f"Отрицательные/нулевые значения в {col}: {len(negative_values)}")

        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {str(e)}")

print("\nПроверка завершена")