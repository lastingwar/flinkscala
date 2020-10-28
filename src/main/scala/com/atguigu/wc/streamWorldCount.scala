package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @author yhm
 * @create 2020-10-27 16:14
 */
object streamWorldCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val params: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    val ds: DataStream[String] = env.socketTextStream(host, port)

    val value: DataStream[(String, Int)] = ds.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    value.print().setParallelism(1)
    "done"
    env.execute("Socket stream word count")
  }
}
