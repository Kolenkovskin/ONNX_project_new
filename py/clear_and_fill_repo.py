import os
import shutil

# Исходная и целевая директории
SRC_DIR = r"C:\Users\User\PycharmProjects\ONNX_bot"
DST_DIR = r"C:\Users\User\PycharmProjects\ONNX_project_new"

# Куда складывать сэмплы
SAMPLES_ROOT = os.path.join(DST_DIR, "data samples")
SPECIAL_FOLDERS = {
    "data_reworked_cleaned": os.path.join(SAMPLES_ROOT, "data_reworked_cleaned"),
    "data_with_indicators": os.path.join(SAMPLES_ROOT, "data_with_indicators"),
    "original": os.path.join(SAMPLES_ROOT, "original"),
}

# Сколько строк копировать для "сэмплов"
N_ROWS = 1000

def copy_file_sample(src, dst, n_rows=N_ROWS):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(src, "r", encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
        for i, line in enumerate(fin):
            fout.write(line)
            if i + 1 >= n_rows:
                break

def main():
    for root, dirs, files in os.walk(SRC_DIR):
        rel_root = os.path.relpath(root, SRC_DIR)
        # Проверяем, не спец-папка ли это
        for special_name, special_dst in SPECIAL_FOLDERS.items():
            if rel_root.startswith(f"csv\\jforex\\{special_name}"):
                # Копируем только сэмплы
                for file in files:
                    src_file = os.path.join(root, file)
                    dst_file = os.path.join(special_dst, os.path.relpath(root, os.path.join(SRC_DIR, "csv", "jforex", special_name)), file)
                    copy_file_sample(src_file, dst_file)
                break
        else:
            # Копируем обычные файлы
            for file in files:
                src_file = os.path.join(root, file)
                dst_file = os.path.join(DST_DIR, rel_root, file)
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                shutil.copy2(src_file, dst_file)

if __name__ == "__main__":
    main()