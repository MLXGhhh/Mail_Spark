package test
import scala.collection.mutable

/**
  * @Description
  * @Author
  * @Date 2019/11/29 0029
  **/
object cehi {
  def main(args: Array[String]): Unit = {
    val set = new mutable.BitSet()
    set.add(4567)
    set.add(457)
    println(set.contains(45678989))
  }
}
