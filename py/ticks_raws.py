import os
import pandas as pd
import random
import datetime

# Путь к файлу с данными
data_file = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_reworked_cleaned\XAUUSD_Ticks_2020.01.01_2025.05.25_cleaned.csv"

# Путь к папке для вывода
output_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\txt"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir,
                           f"tick_data_sample_corrected_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")

# Настройка pandas для полного отображения
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.6f}'.format)


# Функция для получения выборки строк
def sample_rows():
    output = []
    output.append(f"=== Анализ файла: XAUUSD_Ticks_2020.01.01_2025.05.25_cleaned.csv ===")
    output.append(f"Время выполнения: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Проверка столбцов
        df_head = pd.read_csv(data_file, nrows=1, low_memory=False)
        columns = list(df_head.columns)
        output.append(f"Столбцы в файле: {columns}")

        # Поиск столбца с временем
        time_col = None
        for possible_col in ['Time (EET)', 'Time', 'Date', 'Timestamp']:
            if possible_col in columns:
                time_col = possible_col
                break

        # Загрузка файла
        if time_col:
            df = pd.read_csv(data_file, low_memory=False, parse_dates=[time_col])
        else:
            df = pd.read_csv(data_file, low_memory=False)
            output.append("Предупреждение: Столбец с временем не найден, парсинг дат отключен")

        output.append(f"Форма DataFrame: {df.shape} (строк: {df.shape[0]}, столбцов: {df.shape[1]})")

        # Проверка объемов
        if 'AskVolume' in df.columns and 'BidVolume' in df.columns:
            output.append("\n--- Проверка объемов ---")
            output.append(f"Ненулевые AskVolume: {(df['AskVolume'] != 0).sum()}")
            output.append(f"Ненулевые BidVolume: {(df['BidVolume'] != 0).sum()}")
            output.append(f"Уникальные значения AskVolume (первые 10): {list(df['AskVolume'].dropna().unique()[:10])}")
            output.append(f"Уникальные значения BidVolume (первые 10): {list(df['BidVolume'].dropna().unique()[:10])}")
        else:
            output.append("Ошибка: Столбцы 'AskVolume' или 'BidVolume' отсутствуют")

        # Первые 5 строк
        output.append("\n--- Первые 5 строк ---")
        output.append(df.head(5).to_string(index=True, float_format='%.6f'))

        # Последние 5 строк
        output.append("\n--- Последние 5 строк ---")
        output.append(df.tail(5).to_string(index=True, float_format='%.6f'))

        # 15 случайных строк, исключая первые и последние 5
        if len(df) > 10:
            exclude_indices = list(range(5)) + list(range(len(df) - 5, len(df)))
            df_middle = df.drop(exclude_indices)
            if len(df_middle) >= 15:
                random_rows = df_middle.sample(n=15, random_state=random.randint(0, 10000))
                output.append("\n--- 15 случайных строк (исключая первые и последние 5) ---")
                output.append(random_rows.to_string(index=True, float_format='%.6f'))
            else:
                output.append("\n--- 15 случайных строк: Недостаточно строк после исключения ---")
        else:
            output.append("\n--- 15 случайных строк: Файл слишком мал ---")

    except Exception as e:
        output.append(f"Ошибка обработки: {str(e)}")

    output.append(f"\nВремя завершения: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Сохранение в файл
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(output))

    # Подсчет строк
    with open(output_file, 'r', encoding='utf-8') as f:
        line_count = sum(1 for _ in f)
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"\n\n=== Итог: Количество строк в этом файле: {line_count} ===")

    # Вывод в консоль
    for line in output:
        print(line)

    print(f"\nРезультаты сохранены в: {output_file}")


# Запуск
if __name__ == "__main__":
    sample_rows()