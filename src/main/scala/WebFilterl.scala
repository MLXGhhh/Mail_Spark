import Utils.GetHdfsFilesUtils
import org.apache.spark.sql.SparkSession

/**
  * @Description
  * @Author
  * @Date 2019/11/14 0014
  **/
object WebFilterl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MailSpark")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val HdfsPath = "hdfs://159.226.16.173:9000/mail_log/maillog/web-login/APP-17"
    val pathlist = GetHdfsFilesUtils.getFilesDirs(HdfsPath)
    //pathlist.foreach(println)
    pathlist.foreach(path => {
      //循环读取文件夹下的文件 APP-17文件下的日志文件
      val accessRDD = spark.sparkContext.textFile(path.toString)
        .filter(x => (x.contains("requestParameters={isUserConfirmed:true},") && (x.contains("func=mbox:compose,"))))
      //获取文件的日期例如 wmsvr.log.2018-06-02，获取日期为 2018-06-02
      val date: String = GetHdfsFilesUtils.getDate(path.toString)
      println("正在清洗文件："+path)
      val result = accessRDD.map(x => {
        date + " " + x
    })
      println("正在进行结果保存......")
      result.coalesce(1).saveAsTextFile("F:\\[op]= func\\APP-17\\"+date)
    })

  }
}
