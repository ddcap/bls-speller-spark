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
  extends RDD[List[Byte]](prev) with be.ugent.intec.ddecap.Logging {


  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[List[Byte]] = {

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


    // var dataList = ListBuffer[Array[Byte]]();
    // read output data
    // new Thread(s"stderr reader for $cmd") {
    //   override def run(): Unit = {
    //     val stdout = proc.getInputStream
    //     try {
    //         val initSize = 128*1024; //needs to be large enough to not halt the process itself???
    //         var buf = new Array[Byte](initSize)
    //         var wordSize : Byte = 0;
    //         var totalSize = 0
    //         var idx = 0;
    //         var offset = 0;
    //         var arraysize = 0;
    //
    //         var n = stdout.read(buf, offset, initSize)
    //         while (n != -1 ) { // -1 marks EOF
    //           arraysize = n + offset
    //           // process data in buf to n
    //           idx = 0;
    //           wordSize = buf(idx)
    //           totalSize = 2 + wordSize + (wordSize & 0x1);  // if it ends with 1 (uneven length) add a byte for
    //           while(idx + totalSize < arraysize) {
    //             // dataList += buf.slice(idx, idx + totalSize)
    //             idx += totalSize
    //             wordSize = buf(idx)
    //             totalSize = 2 + wordSize + (wordSize & 0x1); // if it ends with 1 (uneven length) add a byte for
    //           }
    //           dataList += buf.slice(0, idx)
    //           offset = 0
    //           for(tmp <- idx to arraysize - 1) {
    //             buf(offset) = buf(tmp)
    //             offset+=1
    //           }
    //           // increase size of buf to be able to read other data again
    //
    //           n = stdout.read(buf, offset, initSize - offset)
    //           while( n == 0) {
    //             Thread.sleep(10)
    //             n = stdout.read(buf, offset, initSize - offset)
    //           }
    //         }
    //         dataList += buf.slice(idx, idx + totalSize)
    //         //process last buf
    //         info(count + " -> datasize: " + dataList.size);
    //     } catch {
    //       case t: Throwable => {childThreadException.set(t);info("error caught " + t)}
    //     } finally {
    //       stdout.close()
    //     }
    //   }
    // }.start()

    // Start a thread to print the process's stderr to ours
    new Thread(s"stderr reader for $cmd") {
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
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread(s"stdin writer for $cmd") {
      override def run(): Unit = {
        TaskContext.setTaskContext(context)
        var fsize = 0
        val out  = new DataOutputStream(proc.getOutputStream());
//        val out = new PrintWriter(new BufferedWriter(
//          new OutputStreamWriter(proc.getOutputStream, Codec.defaultCharsetCodec.name), bufferSize))
        try {
          for (elem <- firstParent[Array[Byte]].iterator(split, context)) {
            fsize += elem.size
            out.write(elem)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          // info("["+ split.index+ "] has written " + fsize + " to stdin")
          out.close()
        }
      }
    }.start()

//    val lines = Source.fromInputStream(proc.getInputStream)(encoding).getLines
    val data = org.apache.commons.io.IOUtils.toByteArray(proc.getInputStream)

    val exitStatus = proc.waitFor()

    // cleanup task working directory if used
    if (workInTaskDirectory) {
      scala.util.control.Exception.ignoring(classOf[IOException]) {
        Utils.deleteRecursively(new File(taskDirectory))
      }
      logDebug(s"Removed task working directory $taskDirectory")
    }
    if (exitStatus != 0) {
      error(s"Subprocess exited with status $exitStatus. " +
        s"Command ran: " + command.mkString(" "))
      throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
        s"Command ran: " + command.mkString(" "))
    }

    val t = childThreadException.get()
    if (t != null) {
     error(s"Caught exception while running pipe() operator. Command ran: $cmd. " +
       s"Exception: ${t.getMessage}")
      proc.destroy()
      throw t
    }


    info("["+ split.index+ "] finished " + procName + " in "+(System.nanoTime-time)/1.0e9+"s")
    // info("data size: " + dataList.size);
    data.toList.grouped(motifSize)
    // dataList.iterator
  }
}
