package ru.phyllosedis.weatherpredict

import com.rabbitmq.client.{ConnectionFactory, DeliverCallback}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}


object Main:
  private val QueuePredictIn = "weather.predict.in"

  @main def runPredictor(): Unit =
    val factory = new ConnectionFactory()
    val rabbitHost = sys.env.getOrElse("RABBIT_HOST", "localhost")
    factory.setHost(rabbitHost)
    factory.setPort(5672)
    factory.setUsername("guest")
    factory.setPassword("guest")

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(QueuePredictIn, true, false, false, null)
    channel.basicQos(1)

    println(s"[Scala Predictor] Подключен к RabbitMQ. Ожидаю задачи в очереди '$QueuePredictIn'...")

    val callback: DeliverCallback = (_, delivery) =>
      val filePathStr = new String(delivery.getBody, StandardCharsets.UTF_8).trim
      println(s"\n[Scala Predictor] Получен файл для прогноза: $filePathStr")

      try
        val inputPath = Path.of(filePathStr)
        if Files.exists(inputPath) then
          val start = System.currentTimeMillis()
          val dailyData = DataAggregator.loadDailyData(inputPath)
          if dailyData.isEmpty then
            println("[Scala Predictor] Ошибка: не удалось распарсить ни одной строки из CSV! Проверьте формат разделителей.")
          else
            println(s"[Scala Predictor] Агрегировано суток: ${dailyData.size} (с ${dailyData.head.date} по ${dailyData.last.date})")
            val forecast = WeatherForecaster.trainAndPredict(dailyData, daysAhead = 30)

            val outputPath = inputPath.getParent.resolve("forecast_30_days.csv")
            WeatherForecaster.saveForecastCsv(forecast, outputPath)

            val elapsed = System.currentTimeMillis() - start
            println(s"[Scala Predictor] Готово за ${elapsed} мс! Прогноз сохранен в: $outputPath")
            println("Первые 5 дней прогноза:")
            forecast.take(5).foreach(p => println(f"  ${p.date}: ${p.predictedTemp}%.2f °C"))
        else
          println(s"[Scala Predictor] Файл не найден: $inputPath")

        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      catch
        case e: Exception =>
          println(s"[Scala Predictor] Ошибка при обучении/прогнозе: ${e.getMessage}")
          e.printStackTrace()
          channel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)

    channel.basicConsume(QueuePredictIn, false, callback, _ => ())

    // Удерживаем главный поток приложения живым
    Thread.currentThread().join()