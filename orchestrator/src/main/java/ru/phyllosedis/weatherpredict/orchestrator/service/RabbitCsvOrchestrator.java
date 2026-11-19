package ru.phyllosedis.weatherpredict.orchestrator.service;

import com.rabbitmq.client.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@ApplicationScoped
public class RabbitCsvOrchestrator {
    private static final String QUEUE_IN = "weather.chunks.in";
    private static final int WORKER_CONCURRENCY = 5;
    private static final String OUTPUT_HEADER =
            "time,temperature_2m,relative_humidity_2m,rain,snowfall,snow_depth," +
                    "wind_speed_10m,wind_speed_100m,wind_direction_10m,wind_direction_100m," +
                    "pressure_msl,surface_pressure,soil_temperature_0_to_7cm,soil_moisture_0_to_7cm," +
                    "temp_ma3,pressure_diff";

    private Connection connection;

    @PostConstruct
    public void init() {
        try {
            String rabbitHost = System.getenv().getOrDefault("RABBIT_HOST", "localhost");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(rabbitHost);
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            this.connection = factory.newConnection();
        } catch (Exception e) {
            throw new RuntimeException("Не удалось подключиться к RabbitMQ", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException ignored) {}
    }


    public Path processCsvFile(Path inputCsv, Path outputCsv, int chunkSize) throws Exception {
        // 1. Читаем исходный Open-Meteo CSV и отрезаем первые 4 строки метаданных/заголовка
        List<String> allLines = Files.readAllLines(inputCsv, StandardCharsets.UTF_8);
        if (allLines.size() <= 4) {
            throw new IllegalArgumentException("Файл пуст или не соответствует формату Open-Meteo");
        }
        List<String> dataLines = allLines.subList(4, allLines.size());

        // 2. Нарезаем на чанки
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < dataLines.size(); i += chunkSize) {
            chunks.add(dataLines.subList(i, Math.min(i + chunkSize, dataLines.size())));
        }

        int totalChunks = chunks.size();
        Semaphore semaphore = new Semaphore(WORKER_CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(totalChunks);
        Map<Integer, String> results = new ConcurrentHashMap<>();

        // Открываем канал для этого сеанса обработки
        try (Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_IN, true, false, false, null);

            // Создаем временную эксклюзивную очередь специально для ответов по этому файлу
            String replyQueueName = channel.queueDeclare().getQueue();

            // Подписываемся на ответы от Clojure-воркеров
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String corrId = delivery.getProperties().getCorrelationId();
                String body = new String(delivery.getBody(), StandardCharsets.UTF_8);

                if (corrId != null) {
                    results.put(Integer.parseInt(corrId), body);
                }
                // Освобождаем слот семафора и уменьшаем счетчик Latch
                semaphore.release();
                latch.countDown();
            };

            String consumerTag = channel.basicConsume(replyQueueName, true, deliverCallback, tag -> {});

            // 3. Отправляем чанки в RabbitMQ через Virtual Threads (Java 25)
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < totalChunks; i++) {
                    final int chunkIndex = i;
                    final String payload = String.join("\n", chunks.get(i));

                    executor.submit(() -> {
                        try {
                            semaphore.acquire(); // Ждем, пока освободится один из 5 воркеров

                            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                                    .correlationId(String.valueOf(chunkIndex))
                                    .replyTo(replyQueueName)
                                    .build();

                            // Канал в RabbitMQ не потокобезопасен на запись, синхронизируем publish
                            synchronized (channel) {
                                channel.basicPublish("", QUEUE_IN, props, payload.getBytes(StandardCharsets.UTF_8));
                            }
                        } catch (Exception e) {
                            semaphore.release();
                            latch.countDown();
                            System.err.println("Ошибка отправки чанка #" + chunkIndex + ": " + e.getMessage());
                        }
                    });
                }

                // 4. Ждем, пока все 5 воркеров перемололи все чанки (таймаут 5 минут на всякий случай)
                boolean completed = latch.await(5, TimeUnit.MINUTES);
                if (!completed) {
                    throw new TimeoutException("Таймаут ожидания ответа от Clojure-воркеров");
                }
            } finally {
                channel.basicCancel(consumerTag);
            }
        }

        // 5. Собираем единый CSV-файл строго в исходном порядке чанков
        Files.createDirectories(outputCsv.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(outputCsv, StandardCharsets.UTF_8)) {
            writer.write(OUTPUT_HEADER);
            writer.newLine();

            for (int i = 0; i < totalChunks; i++) {
                String chunkResult = results.get(i);
                if (chunkResult != null && !chunkResult.isBlank()) {
                    writer.write(chunkResult);
                    if (!chunkResult.endsWith("\n")) {
                        writer.newLine();
                    }
                }
            }
        }
        try (Channel notifyChannel = connection.createChannel()) {
            notifyChannel.queueDeclare("weather.predict.in", true, false, false, null);
            notifyChannel.basicPublish("", "weather.predict.in", null,
                    outputCsv.toAbsolutePath().toString().getBytes(StandardCharsets.UTF_8));
        }

        return outputCsv;
    }
}
