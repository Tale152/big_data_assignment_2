name := "app"
version := "1.0"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.3"
val hadoopVersion = "2.7.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion
)

assemblyMergeStrategy in assembly := {
    case x if Assembly.isConfigFile(x) =>
        MergeStrategy.concat
    case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
        MergeStrategy.rename
    case PathList("META-INF", xs @ _*) =>
        (xs map {_.toLowerCase}) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
                MergeStrategy.discard
            case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
                MergeStrategy.discard
            case "plexus" :: xs =>
                MergeStrategy.discard
            case "services" :: xs =>
                MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
                MergeStrategy.filterDistinctLines
            case _ => MergeStrategy.first
        }
    case _ => MergeStrategy.first
}
