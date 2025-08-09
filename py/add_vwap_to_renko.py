import os
import pandas as pd
import datetime

# Папки
input_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"
output_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"
os.makedirs(output_dir, exist_ok=True)


# Функция для добавления VWAP
def add_vwap(file_path):
    filename = os.path.basename(file_path)
    output_file = os.path.join(output_dir, f"{filename.replace('.csv', '')}_vwap_fixed.csv")

    print(f"Обработка файла: {filename}")
    try:
        # Загрузка файла
        df = pd.read_csv(file_path, low_memory=False, parse_dates=['Time (EET)'])

        # Проверка столбцов
        if 'Close' not in df.columns or 'Volume' not in df.columns:
            print(f"Ошибка: В файле {filename} отсутствуют столбцы 'Close' или 'Volume'")
            return

        # Пересчет VWAP
        if df['Volume'].sum() > 0:
            df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
        else:
            df['VWAP'] = df['Close']  # Запасной вариант
            print(f"Предупреждение: Нулевые объемы в {filename}, VWAP = Close")

        # Сохранение
        df.to_csv(output_file, index=False)
        print(f"VWAP добавлен. Файл сохранен: {output_file}")

    except Exception as e:
        print(f"Ошибка обработки {filename}: {str(e)}")


# Основной код
if __name__ == "__main__":
    renko_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.startswith("XAUUSD_Renko_ONE_PIP") and f.endswith(".csv")
    ]

    print(f"Найдено Renko-файлов: {len(renko_files)}")

    for file_path in renko_files:
        add_vwap(file_path)

    print("Обработка завершена.")