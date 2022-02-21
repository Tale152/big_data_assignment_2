name := "app"
version := "1.0"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.3"
val hadoopVersion = "2.7.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion
)
