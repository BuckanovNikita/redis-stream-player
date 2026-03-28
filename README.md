# boomrdbox

Запись и воспроизведение данных из Redis-стримов. Захватывайте сообщения из потоков в компактный msgpack-файл, а затем воспроизводите их с точным таймингом, регулировкой скорости и корректировкой временных меток.

## Возможности

- **Record** — захват сообщений из нескольких Redis-стримов в один msgpack-файл (с поддержкой SSH-туннелей через именованные инстансы)
- **Play** — воспроизведение записанных сообщений с оригинальным таймингом, регулируемой скоростью и пайплайнингом записи (с защитой от записи в недопустимые хосты)
- **Convert** — экспорт записей в Parquet или CSV для анализа
- **Truncate** — обрезка записей по диапазону ID сообщений или через интерактивный TUI
- **Info** — отображение статистики по каждому стриму из файла записи
- **Setup** — управление именованными инстансами Redis для чтения и списком допустимых хостов для воспроизведения

## Установка

```bash
pip install boomrdbox
```

Или через [uv](https://docs.astral.sh/uv/):

```bash
uv pip install boomrdbox
```

## Быстрый старт

### Запись (record)

Захват сообщений из настроенных стримов:

```bash
boomrdbox record output=recording.msgpack
```

Запись с начала каждого стрима:

```bash
boomrdbox record output=recording.msgpack from_beginning=true
```

Ограничение записи по времени и размеру:

```bash
boomrdbox record max_duration=60 max_size_mb=100
```

### Воспроизведение (play)

Воспроизведение записи с оригинальной скоростью:

```bash
boomrdbox play input=recording.msgpack
```

Воспроизведение на скорости 2x с максимальной задержкой между сообщениями 10 секунд:

```bash
boomrdbox play input=recording.msgpack speed=2.0 max_delay=10
```

### Конвертация (convert)

Экспорт в Parquet или CSV:

```bash
boomrdbox convert input=recording.msgpack output=data.parquet format=parquet
boomrdbox convert input=recording.msgpack output=data.csv format=csv
```

### Обрезка (truncate)

Обрезка записи по диапазону ID сообщений:

```bash
boomrdbox truncate input=recording.msgpack output=slice.msgpack \
    from_id=1709312000000-0 to_id=1709312010000-0
```

Автоматическое определение точки начала, где все стримы активны:

```bash
boomrdbox truncate input=recording.msgpack output=trimmed.msgpack auto_start=true
```

Интерактивный режим с визуальной таймлайн-полосой и клавиатурным управлением:

```bash
boomrdbox truncate input=recording.msgpack output=slice.msgpack interactive=true
```

В интерактивном режиме доступны клавиши:
- `←`/`→` — сдвиг маркера на ±1 минуту
- `Shift+←`/`Shift+→` — грубый сдвиг на ±10 минут
- `Ctrl+←`/`Ctrl+→` — точный сдвиг на ±1 секунду
- `Tab` — переключение между левым и правым маркером
- `t` — ручной ввод времени в формате `HH:MM:SS.mmm`
- `Home`/`End` — прыжок к границе записи
- `Enter` — подтверждение и запуск обрезки
- `Escape`/`q` — выход без действия

### Информация (info)

Отображение статистики записи:

```bash
boomrdbox info input=recording.msgpack
```

### Настройка инстансов для чтения (setup)

Команда `setup` позволяет сохранять именованные подключения к Redis для операций чтения (`record`). Инстансы хранятся в `~/.config/boomrdbox/config.toml`.

#### Добавление инстанса

```bash
boomrdbox setup add staging --host 10.0.0.5 --port 6379 --db 0
```

С SSH-туннелем (для доступа к Redis за бастион-хостом):

```bash
boomrdbox setup add prod --host redis-internal --port 6379 \
    --ssh-host bastion.corp.net --ssh-port 22 --ssh-user deploy --ssh-key ~/.ssh/id_ed25519
```

Интерактивный режим с TUI-формой:

```bash
boomrdbox setup add prod --interactive
```

#### Просмотр и удаление инстансов

```bash
boomrdbox setup list
boomrdbox setup remove staging
```

#### Использование инстанса для записи

```bash
boomrdbox record instance=prod output=recording.msgpack
```

При наличии SSH-туннеля он автоматически устанавливается перед подключением к Redis и закрывается по завершении записи.

### Защита play-команды

Команда `play` записывает данные в Redis и поэтому ограничена списком допустимых хостов. По умолчанию разрешён только хост `redis` (имя сервиса Docker в devcontainer-сети).

Попытка воспроизведения в недопустимый хост приведёт к ошибке:

```
UnsafePlayTargetError: Play command refused: host 'prod-redis' is not in the allowed hosts list ['redis'].
```

#### Управление допустимыми хостами

```bash
boomrdbox setup play-hosts list                 # посмотреть текущий список
boomrdbox setup play-hosts add redis-dev        # добавить хост
boomrdbox setup play-hosts remove redis-dev     # удалить хост
```

Список хранится в `~/.config/boomrdbox/config.toml` в секции `[play]`:

```toml
[play]
allowed_hosts = ["redis"]
```

Инстансы, созданные через `setup add`, предназначены **исключительно для чтения** и никогда не используются командой `play`.

## Конфигурация

Проект использует [Hydra](https://hydra.cc/) с [hydra-zen](https://mit-ll-responsible-ai.github.io/hydra-zen/) для программной настройки конфигурации.

### Подключение к Redis

Переопределение параметров подключения к Redis через конфиг-группы или CLI:

```bash
boomrdbox record redis=prod                # использовать пресет prod
boomrdbox record redis.host=10.0.0.5 redis.port=6380
```

Также поддерживаются переменные окружения `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` и `REDIS_PASSWORD`:

```bash
REDIS_HOST=10.0.0.5 REDIS_PORT=6380 boomrdbox record
REDIS_PASSWORD=secret boomrdbox play input=recording.msgpack
```

Приоритет: CLI-переопределения > переменные окружения > значения по умолчанию из пресета.

### Стримы

Переключение групп стримов или определение стримов в командной строке:

```bash
boomrdbox record streams=events            # использовать пресет events
boomrdbox record 'streams.streams=[{key: mystream}]'
```

Стримы поддерживают корректировку временных меток при воспроизведении:

```yaml
streams:
  - key: sensor:imu
    timestamp_field: receive_ts
    timestamp_mode: bypass      # сохранить оригинальное значение (по умолчанию)
  - key: sensor:camera
    timestamp_field: ts_nano
    timestamp_mode: shift       # сдвинуть к текущему wall-clock времени
```

## Docker Compose для разработки

В комплекте идёт `docker-compose.yaml` для локальной разработки:

```bash
docker compose up -d    # запускает Redis на порту 6389 и RedisInsight на порту 5540
```

Подключение инструментов к dev-серверу Redis:

```bash
boomrdbox record redis.port=6389
```

## Интеграционное тестирование

E2E-тест проверяет полный цикл: заполнение Redis данными → запись → обрезка → воспроизведение → верификация. Требуется Docker.

```bash
bash integration/integration_test.sh              # запуск с автоочисткой
bash integration/integration_test.sh --keep-redis  # оставить Redis для инспекции
```

## Разработка

```bash
# Клонирование и установка
git clone git@github.com:<org>/boomrdbox.git
cd boomrdbox
uv sync --dev

# Запуск проверок
uv run pytest                          # тесты
uv run ruff check .                    # линтер
uv run mypy .                          # проверка типов
uv run pre-commit run --all-files      # все хуки
```
