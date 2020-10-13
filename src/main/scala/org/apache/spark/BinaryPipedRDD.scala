package org.apache.spark


import java.io.{DataOutputStream, File, FilenameFilter, IOException}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicReference

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{HadoopPartition, RDD}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.io.Codec.string2codec
import scala.io.{Codec, Source}
import scala.reflect.ClassTag

// based on PipedRDD from Spark!
class BinaryPipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    procName: String,
    maxMotifLen: Int,
    var logLevel: Level = Level.INFO,
    envVars: Map[String, String] = scala.collection.immutable.Map(),
    separateWorkingDir: Boolean = false)
  extends RDD[(Seq[Byte], (Seq[Byte], Byte))](prev) with be.ugent.intec.ddecap.Logging {


  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Seq[Byte],(Seq[Byte], Byte))] = {

    if(logLevel != null && logLevel != Level.ERROR) {
        Logger.getLogger("be.ugent.intec.ddecap").setLevel(logLevel)
        Logger.getLogger("org.apache.spark.BinaryPipedRDD").setLevel(logLevel)
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
          for (elem <- firstParent[(Seq[Byte], _)].iterator(split, context)) {
            fsize += elem._1.size
            out.write(elem._1.toArray)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          info("["+ split.index+ "] has written " + fsize + " to stdin")
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

    // reads data in stream, without loading it all to memory!! -> way more memory efficient
    // example data format with size of 8:
    //    group    motif <-- (4 bytes since 2 chars per byte and theres 8 chars)
    // 8 - - - -  - - - -  bls
    val wordSize = (maxMotifLen >> 1)
    val totalMotifSize = 2 * wordSize + 2
    val grp: Array[Byte] = Array.fill(wordSize + 1)(0x0);
    val wrd: Array[Byte] = Array.fill(wordSize)(0x0);
    val channel = Channels.newChannel(proc.getInputStream)
    val bufsize = totalMotifSize * 2048 // should be about best performance at this number
    val buf = ByteBuffer.allocate(bufsize);
    var bytesRead = channel.read(buf)
    buf.flip()
    new Iterator[(Seq[Byte], (Seq[Byte], Byte))] {
      def next(): (Seq[Byte], (Seq[Byte], Byte)) = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        buf.get(grp)
        buf.get(wrd)            //    length + grp
        (grp.toVector, (wrd.toVector, buf.get))    // --> (array[byte] , (array[byte], byte ))
      }
      def hasNext(): Boolean = {
        val result = if (buf.position() + totalMotifSize <= buf.limit())
          true
        else {
          buf.compact()
          bytesRead = channel.read(buf)
          if(bytesRead > 0){
            buf.flip()
            // info("read " + bytesRead + " valid bytes")
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
