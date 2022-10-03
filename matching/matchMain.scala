package com.quan.nojoin

object matchMain {
  def main(args: Array[String]): Unit = {
    val crossmatch = new mymatch
    crossmatch.corss("..\\Data\\ELT_data\\ast2\\Tess004_out\\"
    	,"..\\Data\\matchout\\out1","..\\Data\\matchout\\out2")
  }
}
