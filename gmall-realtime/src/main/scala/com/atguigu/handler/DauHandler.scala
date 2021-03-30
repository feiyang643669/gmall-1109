package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date
import java.{lang, util}

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 进行批次内去重
    * @param filterByRedisDStream
    */
  def filterbyGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = {

      //1.将数据转化为k，v ((mid,logDate)log)
      val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })

      //2.groupByKey将相同key的数据聚和到同一个分区中
      val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

      //3.将数据排序并取第一条数据
      val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })
      //4.将数据扁平化
      midAndDateToLogListDStream.flatMap(_._2)
    }
    value
  }

  /**
    * 进行批次间去重
    *
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {

    //方案一：
    //    val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
    //      //创建redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //
    //      //redisKey
    //      val rediskey = "DAU:" + log.logDate
    //
    //      //对比数据，重复的去掉，不重的留下来
    //      val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
    //
    //      //关闭连接
    //      jedisClient.close()
    //
    //      !boolean
    //    })
    //    value
    //方案二：在分区下创建连接（优化）
    //    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
    //      //创建redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //
    //      val logs: Iterator[StartUpLog] = partition.filter(log => {
    //        //redisKey
    //        val rediskey = "DAU:" + log.logDate
    //
    //        //对比数据，重复的去掉，不重的留下来
    //        val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
    //        !boolean
    //      })
    //      //关闭连接
    //      jedisClient.close()
    //      logs
    //    })
    //    value

    //方案三:在每个批次内创建一次连接，来优化连接个数
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.获取redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //2.查redis中的mid
      //获取rediskey
      val rediskey = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

      val mids: util.Set[String] = jedisClient.smembers(rediskey)

      //3.将数据广播至executer端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.根据获取到的mid去重
      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })

      //关闭连接
      jedisClient.close()
      midFilterRDD
    })
    value

  }


  /**
    * 将去重后的数据保存至Redis，为了下一批数据去重用
    *
    * @param startUpLogDStream
    */

  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        //1.创建连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //2.写库
        partition.foreach(log => {
          //redisKey
          val rediskey = "DAU:" + log.logDate
          //将mid存入redis
          jedisClient.sadd(rediskey, log.mid)
        })
        //关闭连接
        jedisClient.close()
      })
    })


  }

}
