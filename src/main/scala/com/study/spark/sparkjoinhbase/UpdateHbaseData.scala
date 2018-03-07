package com.study.spark.sparkjoinhbase

import java.util

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

/**
  * 全量修改hbase中的数据
  * 步骤：
  *   1、全量读取hbase中的表
  *   2、做一系列的ETL
  *   3、把处理过后的数据再写入hbase
  */
object UpdateHbaseData {

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      println(
        """
          |请输入读取表的名称：readTableName
          |请输入保存表的名称：writeTableName
        """.stripMargin)
      System.exit(1)
    }
    val Array(readTableName,writeTableName) = args

    //获取conf
    val conf = HBaseConfiguration.create()
    //设置读取的表
    conf.set(TableInputFormat.INPUT_TABLE,readTableName)
    //设置写入的表
    conf.set(TableOutputFormat.OUTPUT_TABLE,writeTableName)
    //创建sparkConf
    val sparkConf=new SparkConf()
    //设置spark的任务名
    sparkConf.setAppName("read and write for hbase ")
    //创建spark上下文
    val sc = new SparkContext(sparkConf)

    //为job指定输出格式和输出表名
    val newAPIJobConfiguration1 = Job.getInstance(conf)
    newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, writeTableName)
    newAPIJobConfiguration1.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //全量读取hbase表
    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat]
      ,classOf[ImmutableBytesWritable]
      ,classOf[Result]
    )

    //过滤空数据，然后对每一个记录做更新，并转换成写入的格式
    val final_rdd = rdd.filter(!_._2.isEmpty).map {
      case (im, re) => {
        val cells: util.List[Cell] = re.getColumnCells(Array("列族名".toByte),Array("列名".toByte))

        /*****************************************************************************/
        //此段代码未经测试
        import scala.collection.JavaConverters._
        val put = new Put(Bytes.toBytes("rowkey"))
        cells.asScala.foreach(cell=>{
          put.add(cell)
        })
        (new ImmutableBytesWritable, put)
        /*****************************************************************************/
      }
      case _ => (new ImmutableBytesWritable, new Put(Bytes.toBytes("rowkey")))
    }
    //最终在写回hbase表
    final_rdd.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration)
    sc.stop()

  }

}
