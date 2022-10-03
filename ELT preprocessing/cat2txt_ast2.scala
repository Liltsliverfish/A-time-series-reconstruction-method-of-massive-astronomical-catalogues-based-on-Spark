package com.quan.catTotxt
import healpix.essentials.{HealpixBase, Pointing, Scheme}
import java.io.PrintWriter
import scala.io.Source
import scala.util.control.Breaks
/**
 * AST-2数据预处理 ETL AST-2
 */
class cat2txt_ast2 {
  /**
   * 参考星表 reference catalogues
   * @param input input path
   * @param output output path
   * @param flag
   * @param cun_k 计算的层级 healpix level of calculation
   * @param bian_k 根据误差半径选取的边缘块层级 Healpix level for boundary blocks selected according to the error radius
   */
  def newCat2sample(input:String,output:String,flag:Int,cun_k:Int,bian_k:Int): Unit = {
    val hb_cun: HealpixBase = new HealpixBase(Math.pow(2,cun_k).toLong, Scheme.NESTED)
    val hb_bian: HealpixBase = new HealpixBase(Math.pow(2,bian_k).toLong, Scheme.NESTED)
    val d:Double = math.Pi/180

    val file: PrintWriter = new PrintWriter(output)
    var k = 0
    var ra = 0.0
    var dec = 0.0
    var date = ""
    var mag = ""
    var magerr=""
    val source = Source.fromFile(input)
    val strings: Iterator[String] = source.getLines()
    val tail = (bian_k-cun_k)*2
    while(strings.hasNext){
      val line = strings.next()

      if(line.startsWith("# obs")) {
        date = line.substring(24)
      }

      if(!line.startsWith("#")){
        val line_arr = line.split("  ")
        ra = line_arr(11).toDouble
        dec = line_arr(12).toDouble
        mag = line_arr(16)
        magerr = line_arr(17)

        val point: Pointing = new Pointing((90 - dec) * d, ra * d)
        val healpix_cun: java.lang.Long = hb_cun.ang2pix(point) //存储块pix
        val healpix_small:java.lang.Long = hb_bian.ang2pix(point) //计算快pix

        val n_neighbours: Array[Long] = hb_bian.neighbours(healpix_small)
        var neighbour_pix:java.lang.Long = 0
        val bijiao_pre: String = java.lang.Long.toBinaryString(healpix_cun)
        val count: Array[Int] = Array(0, 0, 0, 0,0 ,0,0,0)

        for(m <- List(0,2,4,6)){
          val length = java.lang.Long.toBinaryString(n_neighbours(m)).length
          val bijiao_next = java.lang.Long.toBinaryString(n_neighbours(m)).substring(0,length-tail)
          if(bijiao_next != bijiao_pre){
            count(m) = 1
          }
        }
        val str: Array[String] = input.split("\\\\")
        val source = str(str.length-1).substring(17,21)

        for(j <- List(0,2,4,6)){
          if(count(j) == 1){
            val len = java.lang.Long.toBinaryString(n_neighbours(j)).length
            neighbour_pix = java.lang.Long.parseLong(java.lang.Long.toBinaryString(n_neighbours(j)).substring(0,len-tail),2)
            file.print(flag +" "+source+"_"+ k+ " "+ ra + " " + dec + " "+mag + " " +magerr+" "+neighbour_pix + " "  + "-" + " " +date+" " +0 +" "+ 0+ "\n")
            k = k+1
          }
        }
        file.print(flag +" "+source+"_"+ k +" "+ ra + " " + dec + " "+ mag+" " +magerr+" "+healpix_cun + " " + "-" + " " +date+" " +0 +" "+ 0+ "\n")
        k = k+1
      }
    }
    file.close()
  }

  /**
   * 其他星表 other catalogues
   * @param input
   * @param output
   * @param flag
   * @param cun_k
   * @param bian_k
   */
  def newCat2other(input:String,output:String,flag:Int,cun_k:Int,bian_k:Int): Unit = {

    val hb_cun: HealpixBase = new HealpixBase(Math.pow(2,cun_k).toLong, Scheme.NESTED)
    val hb_bian: HealpixBase = new HealpixBase(Math.pow(2,bian_k).toLong, Scheme.NESTED)
    val d:Double = math.Pi/180
    val file: PrintWriter = new PrintWriter(output)
    var k = 0
    var ra = 0.0
    var dec = 0.0
    var date = ""
    var mag = ""
    var magerr=""

    val source = Source.fromFile(input)
    val strings: Iterator[String] = source.getLines()
    val tail = (bian_k-cun_k)*2
    while(strings.hasNext){
      val line = strings.next()
      if(line.startsWith("# obs")) {
        date = line.substring(24)
      }

      if(!line.startsWith("#")){
        val line_arr = line.split("  ")
        ra =  line_arr(11).toDouble
        dec =  line_arr(12).toDouble
        mag =  line_arr(16)
        magerr=line_arr(17)

        val point: Pointing = new Pointing((90 - dec) * d, ra * d)
        val healpix_cun: java.lang.Long = hb_cun.ang2pix(point) //存储块pix
        val healpix_small:java.lang.Long = hb_bian.ang2pix(point) //计算快pix

        val n_neighbours: Array[Long] = hb_bian.neighbours(healpix_small)
        val bijiao_pre: String = java.lang.Long.toBinaryString(healpix_cun)
        val count: Array[Int] = Array(0, 0, 0, 0,0 ,0,0,0)
        for(m <- List(0,2,4,6)){
          val length = java.lang.Long.toBinaryString(n_neighbours(m)).length
          val bijiao_next = java.lang.Long.toBinaryString(n_neighbours(m)).substring(0,length-tail)
          if(bijiao_next != bijiao_pre){
            count(m) = 1
          }
        }
        val str: Array[String] = input.split("\\\\")
        val source = str(str.length-1).substring(17,21)

        var neighbour_pixStr = ""
        val loop = new Breaks
        loop.breakable {
          for(j <- List(0,2,4,6)){
            if(count(j) == 1){
              val len = java.lang.Long.toBinaryString(n_neighbours(j)).length
              val pix_String = java.lang.Long.parseLong(java.lang.Long.toBinaryString(n_neighbours(j)).substring(0,len - tail),2).toString
              if(neighbour_pixStr.length ==0) neighbour_pixStr = pix_String
              else neighbour_pixStr = neighbour_pixStr + "," + pix_String
            }
            if(neighbour_pixStr.length != 0){
              file.print(flag +" "+source+"_"+ k+" "+ ra + " " + dec + " " +mag+" "+ magerr+" "+healpix_cun + " " + neighbour_pixStr + " " +date+ " " +0 +" "+ 0+ "\n")
              k = k+1
              loop.break()
            }
            if(neighbour_pixStr.length == 0){
              file.print(flag +" "+source+"_"+ k+" "+ra + " " +dec + " " +mag +" "+ magerr+" "+healpix_cun + " " + "-" + " " +date+" " +0 +" "+ 0+ "\n")
              k = k+1
              loop.break()
            }
          }
        }
      }
    }
    file.close()
  }
}
