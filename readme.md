# Polyglot Weather Forecast Pipeline (Java 25 + Clojure + Scala 3)

Распределенная система сбора, очистки и прогнозирования метеоданных на основе исторического архива Open-Meteo (100 000+ записей) с использованием событийной архитектуры и трех языков экосистемы JVM.

## Архитектура системы

```text
[Open-Meteo CSV (100k+ строк)]
              │
              ▼ (HTTP POST /api/weather/process)
┌───────────────────────────────────────────────────────────┐
│ 1. Open Liberty (Java 25) — Оркестратор                   │
│    • Нарезка файла на чанки                               │
│    • Virtual Threads + Semaphore(5) + CountDownLatch      │
└─────────────┬─────────────────────────────▲───────────────┘
              │ weather.chunks.in           │ replyTo (RPC)
              ▼                             │
       ┌──────────────────────────────────────────┐
       │             RabbitMQ Broker              │
       └──────┬─────────────────────────────┬─────┘
              │                             │ weather.predict.in
              ▼                             ▼
┌───────────────────────────┐ ┌─────────────────────────────┐
│ 2. Clojure Workers (x5)   │ │ 3. Scala 3 + Smile ML       │
│    • Очистка данных       │ │    • Суточная агрегация     │
│    • Скользящее среднее   │ │    • Сезонность (sin/cos)   │
│    • Дельта давления      │ │    • Gradient Tree Boost    │
└───────────────────────────┘ └──────────────┬──────────────┘
                                             ▼
                                   [forecast_30_days.csv]
```

## Стек технологий

* **Оркестратор:** Java 25, Jakarta EE 10, Open Liberty, Virtual Threads (`ExecutorService`), `Semaphore`, `CountDownLatch`.
* **Брокер сообщений:** RabbitMQ (паттерн Request/Reply для параллельной обработки чанков + Pub/Sub событие для запуска ML).
* **Обработчик чанков (ETL):** Clojure 1.12, `clojure.data.csv`, Langohr, 5 параллельных реплик.
* **Прогнозирование (ML):** Scala 3.9, sbt, Smile (`GradientTreeBoost`).

## Структура проекта

```text
.
├── data/                       # Исходные датасеты Open-Meteo и результаты прогноза
├── orchestrator/               # Maven-модуль Java 25 + Open Liberty
├── chunk-handler/              # Clojure-воркер для потоковой очистки CSV
├── predictor/                  # sbt-модуль на Scala 3 для обучения модели
└── docker-compose.yaml         # Оркестрация всех 8 контейнеров
```

## Быстрый запуск

1. **Поднимите всю инфраструктуру одной командой:**
   ```bash
   docker compose up --build -d
   ```

2. **Проверьте готовность сервисов:**
    * Веб-панель RabbitMQ: `http://localhost:15672` (логин/пароль: `guest` / `guest`) — в очереди `weather.chunks.in` должно быть 5 активных консьюмеров.
    * Open Liberty API: `http://localhost:9080`

3. **Запустите обработку датасета и генерацию прогноза:**
   ```bash
   curl -X POST "http://localhost:9080/api/weather/process?file=/data/open-meteo-55.78N37.58E151m.csv&chunkSize=10000"
   ```

4. **Результат:**
    * Очищенный и обогащенный файл появится в `data/processed_open-meteo-55.78N37.58E151m.csv`.
    * Прогноз среднесуточной температуры на 30 дней вперед сохранится в `data/forecast_30_days.csv`.
