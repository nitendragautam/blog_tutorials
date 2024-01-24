organization := "com.nitendratech"

name := "sparkprojects"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++={
  val sparkV = "2.3.1"

  Seq (

    "org.apache.spark" % "spark-streaming_2.11" % sparkV ,
    "org.apache.spark" % "spark-core_2.11" % sparkV  ,
    "org.apache.spark" % "spark-sql_2.11" % sparkV ,
    "log4j" % "log4j" % "1.2.17"
  )

}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.first
  case PathList("scala", xs @ _*) => MergeStrategy.discard

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
