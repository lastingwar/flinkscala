package com.atguigu.wc



import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @author yhm
 * @create 2020-10-27 15:38
 */
object worldCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[String] = env.readTextFile("hello.txt")
    ds.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
