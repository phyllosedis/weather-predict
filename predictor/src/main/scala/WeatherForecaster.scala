package ru.phyllosedis.weatherpredict

import smile.data.{DataFrame, Tuple}
import smile.data.formula.Formula
import smile.data.`type`.{DataTypes, StructField, StructType}
import smile.regression.GradientTreeBoost

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.LocalDate
import java.util.Locale
import scala.jdk.CollectionConverters.*

case class ForecastPoint(date: LocalDate, predictedTemp: Double)

object WeatherForecaster:

  private val FeatureNames = Array(
    "t0", "t1", "t2", "t3", "t4", "t5", "t6",
    "hum", "press", "wind", "ma3",
    "sinNext", "cosNext"
  )

  private val FeatureSchema = new StructType(
    FeatureNames.map(name => new StructField(name, DataTypes.DoubleType)) *
  )

  private val TrainSchema = new StructType(
    (FeatureNames :+ "target").map(name => new StructField(name, DataTypes.DoubleType)) *
  )

  def trainAndPredict(daily: Vector[DailyWeather], daysAhead: Int = 30): Vector[ForecastPoint] =
    require(daily.size > 14, s"Недостаточно дней для обучения: ${daily.size}")

    // 1. Формируем обучающую выборку: окно из 7 дней -> прогноз на 8-й день
    val trainRows = (6 until daily.size - 1).map { i =>
      val nextDay = daily(i + 1)
      val values = Array(
        daily(i).avgTemp,
        daily(i - 1).avgTemp,
        daily(i - 2).avgTemp,
        daily(i - 3).avgTemp,
        daily(i - 4).avgTemp,
        daily(i - 5).avgTemp,
        daily(i - 6).avgTemp,
        daily(i).avgHumidity,
        daily(i).avgPressure,
        daily(i).avgWind,
        daily(i).tempMa3,
        nextDay.sinDay,
        nextDay.cosDay,
        nextDay.avgTemp // целевое значение (target)
      )
      Tuple.of(values, TrainSchema)
    }.asJava

    val trainDf = DataFrame.of(trainRows, TrainSchema)
    println(s"[ML] Обучение модели на ${trainDf.size()} суточных окнах...")
    val model = GradientTreeBoost.fit(Formula.lhs("target"), trainDf)

    // 2. Рекурсивный прогноз на 30 дней вперед от самой последней даты в датасете
    var tempWindow = daily.takeRight(7).map(_.avgTemp).reverse.toArray // [t0, t1, ..., t6]
    val lastKnown = daily.last
    var currentDate = lastKnown.date

    (1 to daysAhead).map { _ =>
      currentDate = currentDate.plusDays(1)
      val (sinNext, cosNext) = DataAggregator.seasonalFeatures(currentDate)
      val ma3 = tempWindow.take(3).sum / 3.0

      val inputValues = Array(
        tempWindow(0), tempWindow(1), tempWindow(2), tempWindow(3),
        tempWindow(4), tempWindow(5), tempWindow(6),
        lastKnown.avgHumidity,
        lastKnown.avgPressure,
        lastKnown.avgWind,
        ma3,
        sinNext,
        cosNext
      )

      val inputTuple = Tuple.of(inputValues, FeatureSchema)
      val predictedTemp = model.predict(inputTuple)

      // Сдвигаем окно: предсказанная температура становится новым "сегодня" (t0)
      tempWindow = predictedTemp +: tempWindow.take(6)

      ForecastPoint(currentDate, predictedTemp)
    }.toVector

  def saveForecastCsv(points: Vector[ForecastPoint], outputPath: Path): Unit =
    val header = "date,predicted_temp_c"
    val rows = points.map(p => String.format(Locale.US, "%s,%.2f", p.date.toString, p.predictedTemp))
    Files.write(outputPath, (header +: rows).asJava, StandardCharsets.UTF_8)