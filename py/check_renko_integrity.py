# Метка: CheckRenkoIntegrity_20250529_1944
# Дата и время запуска: 29 мая 2025, 19:44 EEST
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
            df = pd.read_csv(dst_path, sep=';', dtype={'WickPrice': 'object', 'OppositeWickPrice': 'object'})

            # Преобразование числовых столбцов
            for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'WickPrice', 'OppositeWickPrice']:
                if col in df.columns:
                    try:
                        # Проверяем, можно ли преобразовать строку в float
                        def safe_float(x):
                            if pd.isna(x) or x is None:
                                return x
                            if isinstance(x, str):
                                try:
                                    return float(x.replace(',', '.'))
                                except ValueError:
                                    return x  # Оставляем как есть, если не удается преобразовать
                            return x


                        df[col] = df[col].apply(safe_float)

                        # Диагностика проблемных строк (если остались нечисловые значения)
                        problematic = df[df[col].apply(lambda x: isinstance(x, str))]
                        if not problematic.empty:
                            print(f"Проблемные строки в {col}:\n", problematic[[col]].head(5))
                    except Exception as e:
                        print(f"Ошибка преобразования столбца {col}: {str(e)}")

            # 1. Количество строк
            row_count = len(df)
            print(f"Количество строк: {row_count}")

            # 2. Проверка пропусков
            missing_values = df.isnull().sum()
            print(f"Пропуски в данных:\n{missing_values}")

            # 3. Проверка дубликатов
            duplicates = df.duplicated().sum()
            print(f"Количество дубликатов: {duplicates}")

            # 4. Проверка формата времени
            if "Time (EET)" in df.columns:
                try:
                    df["Time (EET)"] = pd.to_datetime(df["Time (EET)"])
                    print("Формат времени корректен")
                except Exception as e:
                    print(f"Ошибка формата времени: {str(e)}")

            # 5. Проверка монотонности времени
            if "Time (EET)" in df.columns:
                time_diff = df["Time (EET)"].diff().dropna()
                non_monotonic = time_diff[time_diff <= pd.Timedelta(0)]
                print(f"Нарушений монотонности времени: {len(non_monotonic)}")
                if len(non_monotonic) > 0:
                    print("Примеры нарушений:\n", non_monotonic.head())

            # 6. Проверка положительности цен и объемов
            price_columns = [col for col in df.columns if col in ["Open", "High", "Low", "Close"]]
            volume_columns = [col for col in df.columns if col in ["Volume"]]

            for col in price_columns:
                negative_prices = df[col][df[col].apply(lambda x: isinstance(x, (int, float)) and x <= 0)]
                print(f"Отрицательные/нулевые значения в {col}: {len(negative_prices)}")

            for col in volume_columns:
                negative_volumes = df[col][df[col].apply(lambda x: isinstance(x, (int, float)) and x < 0)]
                print(f"Отрицательные значения в {col}: {len(negative_volumes)}")

            # 7. Для Renko: проверка WickPrice и OppositeWickPrice
            wick_missing = df["WickPrice"].isnull().sum()
            opp_wick_missing = df["OppositeWickPrice"].isnull().sum()
            print(f"Пропуски в WickPrice: {wick_missing}")
            print(f"Пропуски в OppositeWickPrice: {opp_wick_missing}")

        except Exception as e:
            print(f"Ошибка при обработке файла {file_name}: {str(e)}")

print("\nПроверка завершена")