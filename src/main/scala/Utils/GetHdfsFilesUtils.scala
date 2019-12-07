package Utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

/**
  * @Description
  * @Author
  * @Date 2019/11/14 0014
  **/
object GetHdfsFilesUtils {
  val conf: Configuration = new Configuration()

  /**
    *
    * @Description 获取hdfs文件系统对象
    * @param hdfs路径
    * @return hdfs对象
    * @Date 2019/11/14
    * @Author hky
    **/
  def getHdfs(path: String) = {
    FileSystem.get(new URI(path), conf)
  }

  /**
    *
    * @Description 获取文件目录
    * @param hdfs路径
    * @return 一个文件路径的数组
    * @Date 2019/11/14
    * @Author hky
    **/
  def getFilesDirs(path: String) = {
    val filestatuslist: Array[FileStatus] = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(filestatuslist)
    //stat2Paths将一个FileStatus对象数组转换为Path对象数组。
  }

  def getDate(path: String): String = {
    val date = path.toString.split("/")(7).split("\\.")(2)
    date
  }

  def main(args: Array[String]): Unit = {
    val path: String = "hdfs://159.226.16.173:9000/mail_log/maillog/web-login/APP-17"
    for (path <- getFilesDirs(path)) {
    val date = getDate(path.toString)
      println(date)
    }
  }
}