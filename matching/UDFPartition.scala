package com.quan.utility

import org.apache.spark.Partitioner
import scala.collection.mutable.HashMap

class UDFPartition (val args: Array[String]) extends Partitioner {
  val partitionMap: HashMap[String, Int] = new HashMap[String, Int]()
  var parId = 0
  for (arg <- args) {
    if (!partitionMap.contains(arg)) {
      partitionMap(arg) = parId
      parId += 1
    }
  }

  override def numPartitions: Int = partitionMap.valuesIterator.length
  override def getPartition(key: Any): Int = {
    partitionMap.getOrElse(key.toString,0)
  }
}

