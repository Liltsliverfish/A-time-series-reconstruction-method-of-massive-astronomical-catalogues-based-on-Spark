package com.quan.nojoin
import com.quan.utility.UDFPartition
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
class mymatch {
  def corss(inputdir:String, out1:String, out2:String): Unit = {
    org.apache.log4j.PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("nojoin")
      .set("spark.local.dir","data/sparktmp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired ", "true")
    val sc = new SparkContext(conf)

    val rdd= sc.textFile(inputdir)
      .map(_.split(" "))
      .map(x => (x(6), (x(0), x(1), x(2), x(3), x(4), x(5), x(7), x(8), x(9),x(10))))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val keys: Array[String] = rdd.map(_._1).collect()

    //按照healpix块进行划分，同一healpix块的数据放在一个Spark分区中
    //The partition is based on Healpix blocks. Data from the same HealPix block is stored in a Spark partition
    val rdd1= rdd.partitionBy(new UDFPartition(keys))
      .map(x =>(x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._1, x._2._7, x._2._8, x._2._9, x._2._10))

    //分布式证认计算 Distributed authentication calculation
    val res1 = rdd1.mapPartitions(it => {
      //将新的边缘天体加入到Redis中  new border object will be added to Redis database
      val jedis = new Jedis("localhost", 6379)
      jedis.auth("123456") //密码
      jedis.select(1) //库

      val sampleData = ArrayBuffer[Array[String]]()
      val newData = ArrayBuffer[Array[String]]()
      val hmap = new mutable.HashMap[String,String]()
      var value:Array[String] = null

      //第一行 first line
      value = it.next.productIterator.toArray.map(_.toString)
      val first_flag =  value(0)
      val healpix_id = value(6)
      sampleData += value

      //把“第一天”星表当做参考星表 Use the "Day One" catalogue as a reference catalogue
      var temp_in = 0
      while(temp_in == 0 && it.hasNext){
        value = it.next.productIterator.toArray.map(_.toString)
        if(value(0) == first_flag){
          sampleData += value
        }
        else temp_in = 1
      }

      var is_new = 0
      var next_flag = ""
      if(temp_in == 1){
        //如果Redis中有当前分区的边缘天体，判断日期是否超前
        //If there are border objects of the current partition in Redis, determine if the date is ahead
        if(jedis.exists("healpix_id") && (jedis.llen(healpix_id)!=0)){
          val len = jedis.llen(healpix_id).toInt
          var ll = 0
          while(ll < len){
            val by_str: String = jedis.rpop(healpix_id)
            val by: Array[String] = by_str.split(" ")
            //如果日期超前或者相等，则直接加入参考星表
            //If the date is ahead or equal, it is added to the reference catalogue
            if(by(0).toInt >= value(0).toInt)
              sampleData += by
            //如果日期延后，则需要和参考星表进行证认
            //If the date is delayed, verification with the reference list is required
            else{
              is_new = 0
              for (elem <- sampleData){
                val dis: Double = Math.pow((elem(2).toDouble - by(2).toDouble) * Math.cos((elem(3).toDouble + by(3).toDouble) / 2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - by(3).toDouble,2)
                //如果证认上了，丢弃该星体 If the match is successful, the object is discarded
                if(dis < 0.0000013888){
                  is_new  = 1
                }
              }
              if(is_new == 0){
                //如果没有证认上，1.需要补错过时间段的证认，2.需要参与后续的证认，所以直接加入参考星表参与后续证认，然后再补充错过时间段即可
                //Matching failure: 1. The authentication calculation of the missed time period needs to be made up;
                // 2. The object needs to participate in the subsequent authentication
                sampleData += value
                jedis.lpush(healpix_id + "_by",by_str+" "+value(0))
              }//end if
            }//end else
            ll = ll + 1
          }
        }

        next_flag = value(0)
        is_new = 0
        for (elem <- sampleData){
          val dis: Double = Math.pow((elem(2).toDouble - value(2).toDouble) * Math.cos((elem(3).toDouble + value(3).toDouble) / 2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - value(3).toDouble,2)
          if(dis < 0.0000013888){
            is_new  = 1
            value(9) = "1"
            value(10) = "0"
            if(hmap.contains(elem(1))) hmap(elem(1)) = hmap(elem(1)) +","+value(1)
            else hmap += elem(1) -> value(1)
          }
        }
        //新天体：加入Arraybuffer 如果它是边缘天体也加入Redis
        //New objects: Add to Arraybuffer, If it is a border object also add Redis
        if(is_new == 0){
          newData += value
          if(value(7) != "-") {
            val healpix_nids: Array[String] = value(7).split(",")
            for (healpix_nid <- healpix_nids)
              jedis.lpush(healpix_nid,value(0)+" "+value(1)+" "+value(2)+" "+value(3)+" "+value(4)+" "+value(5)+" "+value(6)+" "+value(7)+" "+value(8)+" "+"0"+" "+"0")
          }
        }
      }
      //同样的方法证认后续星表 The same method is used to verify subsequent catalogues
      while(it.hasNext){
        value = it.next.productIterator.toArray.map(_.toString)
        if(value(0) == next_flag){
          is_new = 0
          for (elem <- sampleData){
            val dis: Double = Math.pow((elem(2).toDouble - value(2).toDouble) * Math.cos((elem(3).toDouble + value(3).toDouble) / 2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - value(3).toDouble,2)
            if(dis < 0.0000013888){
              is_new  = 1
              value(9) = "1"
              value(10) = "0"
              if(hmap.contains(elem(1))) hmap(elem(1)) = hmap(elem(1)) +","+value(1)
              else hmap += elem(1) -> value(1)
            }
          }
          //新天体：加入Arraybuffer 如果它是边缘天体也加入Redis
          //New objects: Add to Arraybuffer, If it is a border object also add Redis
          if(is_new == 0){
            newData += value
            if(value(7) != "-") {
              val healpix_nids = value(7).split(",")
              for (healpix_nid <- healpix_nids)
                jedis.lpush(healpix_nid,value(0)+" "+value(1)+" "+value(2)+" "+value(3)+" "+value(4)+" "+value(5)+" "+value(6)+" "+value(7)+" "+value(8)+" "+"0"+" "+"0")
            }
          }
        }
        else{
          if(jedis.exists(healpix_id) && (jedis.llen(healpix_id)!=0)){
            var ll = 0
            while(ll < jedis.llen(healpix_id).toInt){
              val by_str = jedis.rpop(healpix_id)
              val by = by_str.split(" ")
              if(by(0).toInt >= value(0).toInt)
                sampleData += value
              else{
                is_new = 0
                for (elem <- sampleData){
                  val dis: Double = Math.pow((elem(2).toDouble - by(2).toDouble) * Math.cos((elem(3).toDouble + by(3).toDouble) / 2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - by(3).toDouble,2)
                  if(dis < 0.0000013888){
                    is_new  = 1
                  }
                }
                if(is_new == 0){
                  sampleData += value
                  jedis.lpush(healpix_id + "_by",by_str+" "+value(0))
                }//end if
              }//end else
              ll = ll + 1
            }
          }

          //参考星表的更新 Update the reference catalogues
          val deletedata = ArrayBuffer[Array[String]]()
          for (elem <- sampleData) {
            if (elem(9) == "ok") {
              elem(10) = (elem(10).toInt + 1).toString
            }
            if (elem(9) == "fail") {
              //如果天体连续7次匹配失败并且亮度在阈值范围内，将其从参考星表删除
              //If an object fails to match seven times in a row and the brightness is within the threshold,
              // it will be deleted from the reference list
              if(elem(10) == "6" &&  java.lang.Double.parseDouble(elem(5)) < 18)
                deletedata += elem
              else
                elem(10) = (elem(10).toInt + 1).toString
            }
            if (elem(9) == "1") {
              elem(9) = "ok"
            }
            if (elem(9) == "0") {
              elem(9) = "fail"
              elem(10) = (elem(10).toInt + 1).toString
            }
          }
          if(!deletedata.isEmpty){
            for (elem <- deletedata) {
              sampleData -= elem
            }
          }
          deletedata.clear

          //新天体加入参考星表 New object added to the reference list
          if(!newData.isEmpty){
            for (newdata <- newData) {
              sampleData += newdata
            }
          }
          newData.clear

          next_flag = value(0)
          is_new = 0
          for (elem <- sampleData){
            val dis: Double = Math.pow((elem(2).toDouble - value(2).toDouble) * Math.cos((elem(3).toDouble + value(3).toDouble) / 2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - value(3).toDouble,2)
            if(dis < 0.0000013888){
              is_new  = 1
              if(hmap.contains(elem(1))) hmap(elem(1)) = hmap(elem(1)) +","+value(1)
              else hmap += elem(1) -> value(1) //id1 id2
            }
          }
          if(is_new == 0){
            newData += value
            if(value(7)!= "-") {
              val healpix_nids= value(7).split(",")
              for (healpix_nid <- healpix_nids) {
                jedis.lpush(healpix_nid,value(0)+" "+value(1)+" "+value(2)+" "+value(3)+" "+value(4)+" "+value(5)+" "+value(6)+" "+value(7)+" "+value(8)+" "+"0"+" "+"0")
              }
            }
          }
        }
      }
      //匹配Redis中剩余的边缘天体 Match the remaining edge objects in Redis database
      if(jedis.exists(healpix_id)){
        val len = jedis.llen(healpix_id).toInt
        var ll = 0
        while(ll < len){
          val by_str: String = jedis.rpop(healpix_id)
          val by: Array[String] = by_str.split(" ")
          if(by(0).toInt >= value(0).toInt)
            sampleData += value
          else{
            is_new = 0
            for (elem <- sampleData){
              val dis: Double = Math.pow((elem(2).toDouble - by(2).toDouble) * Math.cos((elem(3).toDouble + by(3).toDouble) / 2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - by(3).toDouble,2)
              if(dis <0.0000013888){
                is_new  = 1
              }
            }
            if(is_new == 0){
              sampleData += value
              jedis.lpush(healpix_id + "_by",by_str+" "+value(0))
            }//end if
          }//end else
          ll = ll + 1
        }
      }
      jedis.close()
      hmap.toIterator
    })
    res1.saveAsTextFile(out1)
    //根据时间补遗漏时间段的匹配计算 Matching calculation of the missed time segment
    val res2 = rdd1.mapPartitions(it =>{

      val sampleData_by = ArrayBuffer[Array[String]]()
      val deleteData_by = ArrayBuffer[Array[String]]()
      val smallData_by = ArrayBuffer[Array[String]]()
      val hmap_by = new mutable.HashMap[String,String]()

      var value_by= it.next.productIterator.toArray.map(_.toString)
      val first_flag =  value_by(0)
      val healpix_id = value_by(6)

      val jedis = new Jedis("localhost", 6379)
      jedis.auth("123456")
      jedis.select(1)
      if(jedis.exists(healpix_id)) {
        val len = jedis.llen(healpix_id).toInt
        var ll = 0
        while (ll < len) {
          val by_str: String = jedis.rpop(healpix_id)
          val by: Array[String] = by_str.split(" ")
          sampleData_by += by
          ll = ll + 1
        }
      }

      if(jedis.exists(healpix_id+"_by")) {
        val len = jedis.llen(healpix_id +"_by").toInt
        var ll = 0
        while (ll < len) {
          val by_str: String = jedis.rpop(healpix_id+"_by")
          val by: Array[String] = by_str.split(" ")
          smallData_by += by
          ll = ll + 1
        }
      }
      for (elem <- sampleData_by if (!sampleData_by.isEmpty)) {
        val dis= Math.pow((elem(2).toDouble - value_by(2).toDouble) * Math.cos((elem(3).toDouble + value_by(3).toDouble) /2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - value_by(3).toDouble,2)
        if(dis < 0.0000013888){
          if(elem(0).toInt >= value_by(0).toInt) {
            deleteData_by += elem
          }
          else{
            if(hmap_by.contains(elem(1))) hmap_by(elem(1)) = hmap_by(elem(1)) +","+value_by(1)
            else hmap_by += elem(1) -> value_by(1)
          }
        }
      }

      for (elem <- deleteData_by if !deleteData_by.isEmpty) {
        sampleData_by -= elem
      }
      deleteData_by.clear()

      for (small <- smallData_by if !smallData_by.isEmpty) {

        if (value_by(0).toInt > small(0).toInt && value_by(0).toInt < small(9).toInt) {
          val dis = Math.pow((small(2).toDouble - value_by(2).toDouble) * Math.cos((small(3).toDouble + value_by(3).toDouble) / 2.0 / 180 * Math.PI), 2) + Math.pow(small(3).toDouble - value_by(3).toDouble, 2)
          if (dis < 0.0000013888) {
            if(hmap_by.contains(small(1))) hmap_by(small(1)) = hmap_by(small(1)) +","+value_by(1)
            else  hmap_by += small(1) -> value_by(1)
          }
        }
      }
      if(!sampleData_by.isEmpty || !smallData_by.isEmpty){
        var isnotExceed = true
        while(it.hasNext){
          value_by= it.next.productIterator.toArray.map(_.toString)
          for (elem <- sampleData_by if !sampleData_by.isEmpty ) {
            val dis_by= Math.pow((elem(2).toDouble - value_by(2).toDouble) * Math.cos((elem(3).toDouble + value_by(3).toDouble) /2.0/180*Math.PI),2)+Math.pow(elem(3).toDouble - value_by(3).toDouble,2)
            if(dis_by < 0.0000013888){
              if(elem(0).toInt >= value_by(0).toInt) deleteData_by += elem
              else {
                if(hmap_by.contains(elem(1))) hmap_by(elem(1)) = hmap_by(elem(1)) +","+value_by(1)
                else  hmap_by += elem(1) -> value_by(1)
              }
            }
          }
          for (elem <- deleteData_by if !deleteData_by.isEmpty) {
            sampleData_by -= elem
          }
          deleteData_by.clear()
          for (small <- smallData_by if isnotExceed) {
            if (value_by(0).toInt > small(0).toInt && value_by(0).toInt < small(9).toInt) {
              val dis = Math.pow((small(2).toDouble - value_by(2).toDouble) * Math.cos((small(3).toDouble + value_by(3).toDouble) / 2.0 / 180 * Math.PI), 2) + Math.pow(small(3).toDouble - value_by(3).toDouble, 2)
              if (dis < 0.0000013888) {
                if(hmap_by.contains(small(1))) hmap_by(small(1)) = hmap_by(small(1)) +","+value_by(1)
                else  hmap_by += small(1) -> value_by(1)
              }
            }
            if(value_by(0).toInt > small(9).toInt)
              isnotExceed = false
          }
        }
      }
      jedis.close()
      hmap_by.toIterator
    })

    res2.saveAsTextFile(out2)
    sc.stop()
  }
}
