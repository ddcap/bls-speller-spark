package org.apache.spark


import org.apache.spark.rdd.HadoopPartition
import scala.reflect.ClassTag
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import org.apache.spark.util.Utils
import java.io.{File, FilenameFilter, IOException, PrintWriter, OutputStreamWriter, BufferedWriter,DataOutputStream}
import scala.io.{Codec, Source}
import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.rdd.RDD
import scala.io.Codec.string2codec
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer

// based on PipedRDD from Spark!
class BinaryPipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    procName: String,
    motifSize: Int,
    var logLevel: Level = Level.INFO,
    envVars: Map[String, String] = scala.collection.immutable.Map(),
    separateWorkingDir: Boolean = false)
  extends RDD[Array[Byte]](prev) with be.ugent.intec.ddecap.Logging {


  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

    if(logLevel != null && logLevel != Level.ERROR) {
        Logger.getLogger("be").setLevel(logLevel)
        Logger.getLogger("org.apache.spark.BinaryPipedRDD").setLevel(logLevel)
        Logger.getLogger("org.apache.spark.ChainedCommandBinaryPipedRDD").setLevel(logLevel)
        Logger.getLogger("org.apache.spark.StringPipedRDD").setLevel(logLevel)
        Logger.getLogger("org.apache.spark.TmpFileBinaryPipedRDD").setLevel(logLevel)
    }
    logLevel = Logger.getLogger(getClass()).getLevel()
    val time = System.nanoTime
    val pb = new ProcessBuilder(command.asJava)
    val cmd = command.mkString(" ")
    info("["+ split.index+ "] running " + cmd)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // for compatibility with Hadoop which sets these env variables
    // so the user code can access the input filename
    if (split.isInstanceOf[HadoopPartition]) {
      val hadoopSplit = split.asInstanceOf[HadoopPartition]
      currentEnvVars.putAll(hadoopSplit.getPipeEnvVars().asJava)
    }

    // When spark.worker.separated.working.directory option is turned on, each
    // task will be run in separate directory. This should be resolve file
    // access conflict issue
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    var workInTaskDirectory = false
    debug("["+ split.index+ "] taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {
      val currentDir = new File(".")
      debug("currentDir = " + currentDir.getAbsolutePath())
      val taskDirFile = new File(taskDirectory)
      taskDirFile.mkdirs()

      try {
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

        // Need to add symlinks to jars, files, and directories.  On Yarn we could have
        // directories and other files not known to the SparkContext that were added via the
        // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
        // are creating here.
        for (file <- currentDir.list(tasksDirFilter)) {
          val fileWithDir = new File(currentDir, file)
          Utils.symlink(new File(fileWithDir.getAbsolutePath()),
            new File(taskDirectory + File.separator + fileWithDir.getName()))
        }
        pb.directory(taskDirFile)
        workInTaskDirectory = true
      } catch {
        case e: Exception => {logError("Unable to setup task working directory: " + e.getMessage +
          " (" + taskDirectory + ")", e); throw e}
      }
    }

    val proc = pb.start()
    val env = SparkEnv.get
    val childThreadException = new AtomicReference[Throwable](null)


    // Start a thread to print the process's stderr to ours
    val stderrReaderThread = new Thread(s"stderr reader for $cmd") {
      override def run(): Unit = {
        val err = proc.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(Codec.defaultCharsetCodec.name).getLines) {
            info("["+ split.index+ "] " + line)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }
    stderrReaderThread.start()

    // Start a thread to feed the process input from our parent's iterator
    val stdinWriterThread = new Thread(s"stdin writer for $cmd") {
      override def run(): Unit = {
        TaskContext.setTaskContext(context)
        var fsize = 0
        val out  = new DataOutputStream(proc.getOutputStream());
//        val out = new PrintWriter(new BufferedWriter(
//          new OutputStreamWriter(proc.getOutputStream, Codec.defaultCharsetCodec.name), bufferSize))
        try {
          for (elem <- firstParent[List[Byte]].iterator(split, context)) {
            fsize += elem.size
            out.write(elem.toArray)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          // info("["+ split.index+ "] has written " + fsize + " to stdin")
          out.close()
        }
      }
    }
    stdinWriterThread.start()

    // interrupts stdin writer and stderr reader threads when the corresponding task is finished.
    context.addTaskCompletionListener { _ =>
      if (proc.isAlive) {
        proc.destroy()
      }

      if (stdinWriterThread.isAlive) {
        stdinWriterThread.interrupt()
      }
      if (stderrReaderThread.isAlive) {
        stderrReaderThread.interrupt()
      }
    }

//    val lines = Source.fromInputStream(proc.getInputStream)(encoding).getLines
    val data = org.apache.commons.io.IOUtils.toByteArray(proc.getInputStream).grouped(motifSize)


    new Iterator[Array[Byte]] {
      def next(): Array[Byte] = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        data.next()
      }

      def hasNext(): Boolean = {
        val result = if (data.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          cleanup()
          if (exitStatus != 0) {
            error(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          info("["+ split.index+ "] finished " + procName + " in "+(System.nanoTime-time)/1.0e9+"s")
          false
        }
        propagateChildException()
        result
      }
      private def cleanup(): Unit = {
        // cleanup task working directory if used
        if (workInTaskDirectory) {
          scala.util.control.Exception.ignoring(classOf[IOException]) {
            Utils.deleteRecursively(new File(taskDirectory))
          }
          logDebug(s"Removed task working directory $taskDirectory")
        }
      }

      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          error(s"Caught exception while running pipe() operator. Command ran: $cmd. " +
            s"Exception: ${t.getMessage}")
          proc.destroy()
          cleanup()
          throw t
        }
      }
    }
  }
}
