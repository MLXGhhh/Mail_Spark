import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,SparkSession}

/**
  * @Description 基于恶意IP黑名单的邮件用户异常检测
  * @Author
  * @Date 2019/11/29 0029
  **/
object MaliciousIP {

  case class login(time: String, user: String, ip: String)
/**
  *@Description 进行日志数据的格式化，将登陆成功日志解析为 时间-用户-IP形式
  *@param log
  *@return
  *@Date 2019/12/6 0006
  *@Author hky
  **/
  def parseLog(log: String)= {
    val fields: Array[String] = log.split(" ")
    val time = fields(0).split("\\(")(1) + " " + fields(1).substring(0, 5)
    val user = fields(3)
    val ip = fields(5).split("]")(0).substring(1, fields(5).split("]")(0).length)
    login(time, user, ip)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MaliciousIP")
      .getOrCreate()
    //限定spark日志输出级别
    spark.sparkContext.setLogLevel("WARN")
    //获取登录日志路径
    val InputPath: String = this.getClass.getResource("imapsvr.log").getPath
    //获取恶意IP路径
    val MaliciousIPPath: String = this.getClass.getResource("MaliciousIP_Data").getPath
    //导入隐式转换
    import spark.implicits._ 
    //获取登录日志
    val accessRDD: RDD[String] = spark.sparkContext.textFile("E:\\文件\\邮件日志\\登入登出日志\\imap-filter\\APP-26\\APP-26\\*")
    //获取恶意IP库，并转换至dataframe
    val MaliciousDF  = spark.read.textFile(MaliciousIPPath).toDF("Blacklist")  /** 恶意IP表 */
    //过滤登录日志，并进行日志的解析 去重
    val login: RDD[login] = accessRDD
      .filter(x => (x.length > 1 && x.contains("login success")))
      .map(parseLog)
      .filter(_.ip != null)
      .distinct()
    //将数据处理结果转换成DataFrame形式，并指定数据列
    val frame: DataFrame = login.toDF("time", "user", "ipv4")  /** 登陆数据表 */
    //将登陆数据表和恶意IP表进行左连接操作，并进行数据筛选
    val resultDF: DataFrame = frame.join(MaliciousDF,frame("ipv4") === MaliciousDF("Blacklist")).toDF().select("time","user","ipv4").toDF()

    val usercount: Long = login.map(x => x.user).distinct().count()
    println("登录账户数量为：" + usercount)

    val count: Long = resultDF.select("user").toDF().rdd.distinct().count()
    println("异常账户数量为：" + count)
    println("经检测异常用户行为如下：" )
    resultDF.show(30)
    //resultDF.coalesce(1).write.csv("F:\\[op]= func\\APP-26")
  }
}
