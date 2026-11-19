package ru.phyllosedis.weatherpredict

import java.nio.file.{Files, Path}
import java.time.{Instant, LocalDate, ZoneOffset}
import scala.jdk.CollectionConverters.*


case class DailyWeather(
                         date: LocalDate,
                         avgTemp: Double,
                         avgHumidity: Double,
                         avgPressure: Double,
                         avgWind: Double,
                         tempMa3: Double,
                         sinDay: Double,
                         cosDay: Double
                       )

object DataAggregator:

  def loadDailyData(csvPath: Path): Vector[DailyWeather] =
    val lines = Files.readAllLines(csvPath).asScala.toVector
    // Пропускаем заголовок (1-я строка) и парсим колонки:
    // 0:time, 1:temp, 2:hum, 6:wind10, 10:press_msl, 14:temp_ma3
    val hourlyRecords = lines.drop(1).flatMap { line =>
      val cols = line.split(",")
      if cols.length >= 15 then
        try
          val epochSeconds = cols(0).trim.toLong
          val date = Instant.ofEpochSecond(epochSeconds).atZone(ZoneOffset.UTC).toLocalDate
          Some((
            date,
            cols(1).trim.toDouble,
            cols(2).trim.toDouble,
            cols(10).trim.toDouble,
            cols(6).trim.toDouble,
            cols(14).trim.toDouble
          ))
        catch case _: Exception => None
      else None
    }

    // Группируем по дате и усредняем показатели за сутки
    hourlyRecords
      .groupBy(_._1)
      .toVector
      .sortBy(_._1)
      .map { case (date, rows) =>
        val n = rows.size.toDouble
        val (sinD, cosD) = seasonalFeatures(date)
        DailyWeather(
          date = date,
          avgTemp = rows.map(_._2).sum / n,
          avgHumidity = rows.map(_._3).sum / n,
          avgPressure = rows.map(_._4).sum / n,
          avgWind = rows.map(_._5).sum / n,
          tempMa3 = rows.map(_._6).sum / n,
          sinDay = sinD,
          cosDay = cosD
        )
      }

  def seasonalFeatures(date: LocalDate): (Double, Double) =
    val angle = 2.0 * Math.PI * date.getDayOfYear / 365.25
    (Math.sin(angle), Math.cos(angle))