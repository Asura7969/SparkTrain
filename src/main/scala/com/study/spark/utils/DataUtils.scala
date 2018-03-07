package com.study.spark.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by zhang on 2017/11/22.
  * 156.187.29.132	2017-11-20 00:39:26	"GET www/2 HTTP/1.0"	-	200
  * 2017-11-20 00:39:26 => 20171120
  */
object DataUtils {
    val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val TAG_FORMAT = FastDateFormat.getInstance("yyyyMMdd")

    /**
      * 把当前时间转换成时间戳
      * @param time
      * @return
      */
    def getTime(time:String) ={
        YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
    }

    def parseToMin(time:String) ={
        TAG_FORMAT.format(new Date(getTime(time)))
    }


    def main(args: Array[String]): Unit = {
        print(parseToMin("2017-11-20 00:39:26"))
    }


}
