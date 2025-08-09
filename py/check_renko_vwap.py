import os
import pandas as pd

# Папка с Renko-файлами
data_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"

# Настройка pandas
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.6f}'.format)
pd.set_option('display.max_columns', None)


# Функция для проверки VWAP в файле
def check_vwap(file_path):
    filename = os.path.basename(file_path)
    print(f"\n=== Проверка файла: {filename} ===")

    try:
        # Загрузка первых 1000 строк для проверки первых 10
        df_head = pd.read_csv(file_path, low_memory=False, parse_dates=['Time (EET)', 'EndTime'], nrows=1000)
        print("VWAP пропуски:", df_head['VWAP'].isnull().sum())
        print("\nVWAP vs Close (первые 10 строк):")
        print(df_head[['VWAP', 'Close']].head(10))

        # Загрузка строк 5,000,000–5,000,010
        df_middle = pd.read_csv(file_path, low_memory=False, parse_dates=['Time (EET)', 'EndTime'],
                                skiprows=range(1, 5000000), nrows=11)
        print("\nVWAP vs Close (середина, строки 5,000,000–5,000,010):")
        print(df_middle[['VWAP', 'Close']].iloc[:11])

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
        check_vwap(file_path)

    print("\nПроверка завершена.")