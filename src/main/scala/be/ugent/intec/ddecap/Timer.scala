package be.ugent.intec.ddecap


object Timer extends Logging {
  var time: Long = 0
  var totaltime: Long = 0

  def time[A](title: String, f: => A) = {
    val s = System.nanoTime
    val ret = f
    info("[" + title + "] time (s): "+(System.nanoTime-s)/1.0e9)
    ret
  }

  def startTime() = {
    totaltime = System.nanoTime
    time = System.nanoTime
  }
  def measureTotalTime(title: String) :String = {
    "[" + title + "] total time (s): "+(System.nanoTime-totaltime)/1.0e9
  }
  def measureTime(title: String) :String = {
    val res = "[" + title + "] time (s): "+(System.nanoTime-time)/1.0e9
    time = System.nanoTime
    res
  }
}
