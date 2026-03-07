# boomrdbox

Запись и воспроизведение данных из Redis-стримов. Захватывайте сообщения из потоков в компактный msgpack-файл, а затем воспроизводите их с точным таймингом, регулировкой скорости и корректировкой временных меток.

## Возможности

- **Record** — захват сообщений из нескольких Redis-стримов в один msgpack-файл
- **Play** — воспроизведение записанных сообщений с оригинальным таймингом, регулируемой скоростью и пайплайнингом записи
- **Convert** — экспорт записей в Parquet или CSV для анализа
- **Truncate** — обрезка записей по диапазону ID сообщений
- **Info** — отображение статистики по каждому стриму из файла записи

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

### Информация (info)

Отображение статистики записи:

```bash
boomrdbox info input=recording.msgpack
```

## Конфигурация

Проект использует [Hydra](https://hydra.cc/) с [hydra-zen](https://mit-ll-responsible-ai.github.io/hydra-zen/) для программной настройки конфигурации.

### Подключение к Redis

Переопределение параметров подключения к Redis через конфиг-группы или CLI:

```bash
boomrdbox record redis=prod                # использовать пресет prod
boomrdbox record redis.host=10.0.0.5 redis.port=6380
```

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
