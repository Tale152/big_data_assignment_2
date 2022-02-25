package utils

import java.nio.file.Paths

/**
 * List of constants used by the application.
 */
object Const {
  val appName = "K-Means"
  val jarPath: String = Paths.get("target", "scala-2.12", "app_2.12-1.0.jar").toString
}
