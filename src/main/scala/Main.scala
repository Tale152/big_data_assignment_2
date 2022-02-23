import kMeans.Computation.{kMeans, printConfiguration}
import utils.{Const, ContextFactory}
import utils.LogEnabler.logSelectedOption
import utils.ArgsProvider

object Main {

    def main(args: Array[String]): Unit = {
      ArgsProvider.setArgs(args)
      logSelectedOption(ArgsProvider.logFlag)
      printConfiguration()
      val sc = ContextFactory.create(Const.appName, ArgsProvider.sparkMaster, Const.jarPath)
      kMeans(sc)
      sc.stop()
    }
}