# Метка: UploadToGitHubPyAndSplitFiles_20250530_0601
# Дата и время запуска: 30 мая 2025, 06:01 EEST
import os
import git
from github import Github, GithubException
from datetime import datetime

# Параметры
project_dir = r"C:\Users\Estal\PycharmProjects\ONNX_bot"
py_dir = os.path.join(project_dir, "py")  # Папка с Python-скриптами
split_files_dir = os.path.join(project_dir, "csv", "jforex", "split_files")  # Папка с разделенными данными
github_token = os.getenv("GITHUB_TOKEN")  # Получаем токен из переменной окружения
github_username = "Kolenkovskin"
repo_name = "ONNX_project_new"  # Новое имя репозитория
commit_message = "Initial upload of py and split_files to GitHub"

# Проверка наличия токена
if not github_token:
    raise Exception("Переменная окружения GITHUB_TOKEN не найдена. Установите токен в переменных окружения.")
else:
    print(f"GitHub токен успешно считан (первые 5 символов): {github_token[:5]}...")

# Вывод из list_content.py для добавления в README.md
readme_content = """[2025-05-30 05:10:00.990030] Список содержимого всех папок и подпапок в C:\\Users\\Estal\\PycharmProjects\\ONNX_bot (кроме {'.git', '.idea', '.venv'})
Найдено следующее содержимое:
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_1 Min_Merged.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_30 Mins_Merged.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Daily_Ask_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Daily_Bid_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Hourly_Ask_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Hourly_Bid_2020.01.01_2025.05.25_processed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\data_reworked\\XAUUSD_Ticks_2024.01.01_2025.05.25_cleaned.csv
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_1 Min_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_1 Min_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_30 Mins_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_30 Mins_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Daily_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Daily_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Hourly_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Hourly_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\original\\XAUUSD_Ticks_2024.01.01_2025.05.25.csv
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_1 Min_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_1 Min_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_15 Mins_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_15 Mins_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_30 Mins_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_30 Mins_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_4 Hours_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_4 Hours_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_5 Mins_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_5 Mins_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Daily_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Daily_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Hourly_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Hourly_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Renko_ONE_PIP_Ticks_Ask_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Renko_ONE_PIP_Ticks_Bid_2020.01.01_2025.05.25_trimmed.csv
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\jforex\\split_files\\XAUUSD_Ticks_2024.01.01_2025.05.25_trimmed.csv
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\csv\\orderbook
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\exe
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\exe\\chromedriver.exe
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\h5
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\mqh
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\onnx
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\png
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_15min_gaps.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_columns.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_data_integrity.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_jforex_original.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_mt5_orderbook.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_original_files.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_original_suitability.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_original_suitability_short.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_renko_integrity.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_renko_wick.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_ticks.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\check_yfinance_timeframes.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\collect_orderbook.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\count_split_files.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\list_content.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\list_folders.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\split_original_files.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\split_renko.py
  Файл: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\upload_to_github.py
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\py\\__pycache__
Папка: C:\\Users\\Estal\\PycharmProjects\\ONNX_bot\\txt

Получение списка установленных библиотек...
Установленные библиотеки:
 - absl-py                 2.2.2
 - astunparse              1.6.3
 - attrs                   25.3.0
 - beautifulsoup4          4.13.4
 - certifi                 2025.4.26
 - cffi                    1.17.1
 - charset-normalizer      3.4.2
 - click                   8.2.1
 - cloudpickle             3.1.1
 - colorama                0.4.6
 - coloredlogs             15.0.1
 - contourpy               1.3.2
 - cryptography            45.0.3
 - curl_cffi               0.11.1
 - cycler                  0.12.1
 - Cython                  3.1.0
 - dask                    2025.5.1
 - Deprecated              1.2.18
 - filelock                3.18.0
 - flatbuffers             25.2.10
 - fonttools               4.58.0
 - frozendict              2.4.6
 - fsspec                  2025.3.2
 - gast                    0.6.0
 - gitdb                   4.0.12
 - GitPython               3.1.44
 - google-pasta            0.2.0
 - grpcio                  1.71.0
 - h11                     0.16.0
 - h5py                    3.13.0
 - humanfriendly           10.0
 - idna                    3.10
 - Jinja2                  3.1.6
 - joblib                  1.5.0
 - keras                   3.9.2
 - kiwisolver              1.4.8
 - libclang                18.1.1
 - locket                  1.0.0
 - Markdown                3.8
 - markdown-it-py          3.0.0
 - MarkupSafe              3.0.2
 - matplotlib              3.10.3
 - mdurl                   0.1.2
 - MetaTrader5             5.0.4993
 - ml-dtypes               0.3.2
 - mpmath                  1.3.0
 - multitasking            0.0.11
 - namex                   0.0.9
 - networkx                3.4.2
 - numpy                   1.26.4
 - onnx                    1.17.0
 - onnxruntime             1.17.0
 - opt_einsum              3.4.0
 - optree                  0.15.0
 - outcome                 1.3.0.post0
 - packaging               25.0
 - pandas                  2.2.3
 - partd                   1.4.2
 - peewee                  3.18.1
 - pillow                  11.2.1
 - pip                     25.1.1
 - platformdirs            4.3.8
 - protobuf                3.20.3
 - pyarrow                 20.0.0
 - pycparser               2.22
 - PyGithub                2.6.1
 - Pygments                2.19.1
 - PyJWT                   2.10.1
 - PyNaCl                  1.5.0
 - pyparsing               3.2.3
 - pyreadline3             3.5.4
 - PySocks                 1.7.1
 - python-dateutil         2.9.0.post0
 - pytz                    2025.2
 - PyYAML                  6.0.2
 - requests                2.32.3
 - rich                    14.0.0
 - scikit-learn            1.6.1
 - scipy                   1.15.3
 - selenium                4.33.0
 - setuptools              80.7.1
 - six                     1.17.0
 - smmap                   5.0.2
 - sniffio                 1.3.1
 - sortedcontainers        2.4.0
 - soupsieve               2.7
 - sympy                   1.14.0
 - ta                      0.11.0
 - ta-lib                  0.6.3
 - tensorboard             2.16.2
 - tensorboard-data-server 0.7.2
 - tensorflow              2.16.1
 - tensorflow-intel        2.16.1
 - termcolor               3.1.0
 - tf2onnx                 1.16.1
 - threadpoolctl           3.6.0
 - toolz                   1.0.0
 - torch                   2.7.0
 - trio                    0.30.0
 - trio-websocket          0.12.2
 - typing_extensions       4.13.2
 - tzdata                  2025.2
 - urllib3                 2.4.0
 - websocket-client        1.8.0
 - websockets              15.0.1
 - Werkzeug                3.1.3
 - wheel                   0.45.1
 - wrapt                   1.17.2
 - wsproto                 1.2.0
 - yfinance                0.2.61

Сканирование завершено
"""

print(f"[{datetime.now()}] Автоматизация загрузки py и split_files на GitHub")

# Проверка существования папок py и split_files
if not os.path.exists(py_dir):
    raise Exception(f"Папка {py_dir} не найдена")
if not os.path.exists(split_files_dir):
    raise Exception(f"Папка {split_files_dir} не найдена")

# Шаг 1: Инициализация нового Git-репозитория
try:
    repo = git.Repo(project_dir)
    print("Git-репозиторий уже существует, но должен быть удалён ранее")
except git.exc.NoSuchPathError:
    raise Exception(f"Папка проекта {project_dir} не найдена")
except git.exc.InvalidGitRepositoryError:
    print("Инициализация нового Git-репозитория")
    repo = git.Repo.init(project_dir)

# Шаг 2: Создание файла .gitignore
gitignore_path = os.path.join(project_dir, ".gitignore")
if not os.path.exists(gitignore_path):
    with open(gitignore_path, "w") as f:
        f.write(".venv/\n__pycache__/\n*.pyc\ncsv/jforex/original/\ncsv/jforex/data_reworked/\n")
    print("Создан файл .gitignore с исключениями original и data_reworked")

# Шаг 3: Добавление содержимого папок py и split_files
repo.git.add(py_dir)
repo.git.add(split_files_dir)
repo.git.add(gitignore_path)  # Добавляем .gitignore
print("Файлы из py и split_files добавлены в Git")

# Шаг 4: Создание файла README.md
readme_path = os.path.join(project_dir, "README.md")
with open(readme_path, "w", encoding="utf-8") as f:
    f.write(readme_content)
repo.git.add(readme_path)
print("Создан и добавлен файл README.md")

# Шаг 5: Создание коммита
repo.index.commit(commit_message)
print(f"Создан коммит: {commit_message}")

# Шаг 6: Проверка авторизации в GitHub
try:
    g = Github(github_token)
    user = g.get_user()
    print(f"Авторизация в GitHub успешна, пользователь: {user.login}")
except GithubException as e:
    raise Exception(f"Ошибка авторизации в GitHub: {e}")

# Шаг 7: Создание нового репозитория на GitHub
try:
    repo_github = user.create_repo(repo_name, private=False, description="ONNX_project_new with py and split_files")
    print(f"Создан новый репозиторий {repo_name} на GitHub")
except GithubException as e:
    if e.status == 422:  # Репозиторий уже существует
        print(f"Репозиторий {repo_name} уже существует на GitHub, используем его")
        repo_github = user.get_repo(repo_name)
    else:
        raise Exception(f"Ошибка при создании репозитория: {e}")

# Шаг 8: Настройка удаленного репозитория
remote_url = f"https://{github_username}:{github_token}@github.com/{github_username}/{repo_name}.git"
try:
    remote = repo.remote(name="origin")
    remote.set_url(remote_url)
except ValueError:
    remote = repo.create_remote("origin", remote_url)

# Шаг 9: Пуш изменений
print("Пуш изменений на GitHub...")
try:
    repo.git.push("origin", "main")
    print("Файлы из py и split_files успешно загружены на GitHub")
    print(f"Ссылка на репозиторий: https://github.com/{github_username}/{repo_name}")
except git.exc.GitCommandError as e:
    raise Exception(f"Ошибка при выполнении git push: {e}")