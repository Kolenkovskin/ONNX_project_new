import os
import datetime

# Путь к папке с данными
data_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\csv\jforex\data_reworked\data_with_indicators"

# Путь к выходному файлу
output_dir = r"C:\Users\User\PycharmProjects\ONNX_bot\txt"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir,
                           f"file_list_with_sizes_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")

# Получение списка файлов и их размеров
try:
    files = [f for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f))]
    output = [f"Список файлов в папке {data_dir} ({len(files)} файлов):"]
    output.append(f"Время сканирования: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append("")

    for i, f in enumerate(files):
        file_path = os.path.join(data_dir, f)
        size_bytes = os.path.getsize(file_path)
        size_kb = size_bytes / 1024
        size_mb = size_kb / 1024
        output.append(f"{i + 1}. {f}")
        output.append(f"   Размер: {size_bytes} байт, {size_kb:.2f} КБ, {size_mb:.2f} МБ")

    # Вывод в консоль
    for line in output:
        print(line)

    # Сохранение в файл
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(output))
        f.write(f"\n\nИтог: Количество строк в этом файле: {len(output)}")

    print(f"\nРезультаты сохранены в: {output_file}")

except Exception as e:
    error_msg = f"Ошибка при сканировании папки: {str(e)}"
    print(error_msg)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(error_msg)
        f.write(f"\n\nИтог: Количество строк в этом файле: 1")