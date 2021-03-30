package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {

    //TODO 1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //TODO 2.创建StreamingContext
    //    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //TODO 3.连接kafka
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //TODO 4.将json数据转换为样例类，方便解析
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将json转换为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        //补充字段分别为 logdate loghour
        //yyyy-MM-dd HH
        val str: String = sdf.format(new Date(startUpLog.ts))

        //补充字段 logdate yyyy-MM-dd
        startUpLog.logDate = str.split(" ")(0)

        //补充字段 loghour HH
        startUpLog.logHour = str.split(" ")(1)

        startUpLog
      })
    })

    //    startUpLogDStream.cache()

    //TODO 5.进行批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    //    filterByRedisDStream.cache()
    //    startUpLogDStream.count().print()
    //    filterByRedisDStream.count().print()

    //TODO 6.进行批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterbyGroup(filterByRedisDStream)
    //    filterByGroupDStream.cache()
    //
    //    filterByGroupDStream.count().print()

    //TODO 7.将去重后的数据保存至Redis，为了下一批数据去重用
    DauHandler.saveMidToRedis(filterByGroupDStream)


    //TODO 8.将数据保存至Hbase
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL1109_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //测试 只使用map时不能触发任务，因为map是转换算子
    //    kafkaDStream.map(record=>{
    //      println(record.value())
    //    })
    //    kafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>{
    //        println(record.value())
    //      })
    //    })

    ssc.start()
    ssc.awaitTermination()

  }

}
