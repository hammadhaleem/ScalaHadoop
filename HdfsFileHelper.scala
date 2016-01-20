package util

import java.io._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.mutable.{ListBuffer => MutableList}

class HdfsFileHelper {

  private var fileSystem : FileSystem = null

  def this(sc: SparkContext){
    this()
    this.fileSystem = FileSystem.get(sc.hadoopConfiguration)
  }

  def mkdirs(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path))
      fileSystem.mkdirs(path)
  }

  def getFiles(folderPath: String) : MutableList[String] = {
    val path = new Path(folderPath)
    val ret = MutableList[String]()
    if(!fileSystem.exists(path))
      MutableList[String]()
    else {
      val files = fileSystem.listFiles(path ,true)
      while (files.hasNext) {
        val status = files.next()
        if (status.isFile && !status.getPath.getName.startsWith("_") ) {
          ret.append(status.getPath.toString)
        }
      }
    }
    ret
  }

  def createNewFile(filepath: String): Boolean = {
    val file = new File(filepath)
    if (fileSystem.createNewFile(new Path(file.getAbsolutePath)))
      true
    else
      false
  }

  def reader(filenamePath : String): Array[String] ={
    val out = fileSystem.open(new Path(filenamePath))
    val data : Array[String]= out.readUTF().split("\n")
    data
  }

  def appendToFile(tofilepath: String, data: MutableList[String]): Unit = {
    if(!fileSystem.exists(new Path(tofilepath)))
      createNewFile(tofilepath)
    val out = fileSystem.append(new Path(tofilepath))
    for(line <- data)
      out.writeChars(line)
    out.close
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def deleteFolder(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def close() = {
    fileSystem.close()
  }

}
