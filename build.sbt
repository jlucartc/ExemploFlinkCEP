name := "FlinkCEP"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.flink/flink-cep
libraryDependencies += "org.apache.flink" %% "flink-cep" % "1.9.0"

// https://mvnrepository.com/artifact/org.apache.flink/flink-cep-scala
libraryDependencies += "org.apache.flink" %% "flink-cep-scala" % "1.9.0"

// https://mvnrepository.com/artifact/org.apache.flink/flink-runtime
libraryDependencies += "org.apache.flink" %% "flink-runtime" % "1.9.0" % Test

// https://mvnrepository.com/artifact/org.apache.flink/flink-scala
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.9.0"

// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.9.0"

// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.9.0"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.2" % Test

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.10"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.9"

// https://mvnrepository.com/artifact/org.apache.flink/flink-runtime-web
libraryDependencies += "org.apache.flink" %% "flink-runtime-web" % "1.9.0" % Test
