package utils

import java.nio.file.Paths

object Const {
  val appName = "K-Means"
  val jarPath: String = Paths.get("target", "scala-2.12", "app_2.12-1.0.jar").toString
}
