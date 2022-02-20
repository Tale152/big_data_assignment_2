name := "app"
version := "1.0"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.3"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.hadoop" % "hadoop-common" % "2.7.4"
)
