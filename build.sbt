name := "app"
version := "1.0"
scalaVersion := "2.12.10"
libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.12" % "3.0.3",
"org.apache.hadoop" % "hadoop-common" % "2.7.3"
)
