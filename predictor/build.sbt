scalaVersion := "3.9.0"

lazy val root = rootProject
  .settings(
    name := "predictor",
    idePackagePrefix := Some("ru.phyllosedis.weatherpredict"),
    libraryDependencies ++= Seq(
      //You can add library dependencies here, for example,
      //"org.scalatest" %% "scalatest" % "3.2.19" % Test,
      //"org.scalameta" %% "munit" % "1.2.3" % Test
      "com.github.haifengl" % "smile-core" % "3.1.1",
      "com.rabbitmq" % "amqp-client" % "5.25.0"
    )
  )
