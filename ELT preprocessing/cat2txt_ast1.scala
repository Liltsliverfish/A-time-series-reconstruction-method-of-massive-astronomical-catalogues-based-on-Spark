package com.quan.catTotxt
import healpix.essentials.{HealpixBase, Pointing, Scheme}
import nom.tam.fits.{BinaryTableHDU, Fits}
import java.io.{BufferedOutputStream, FileOutputStream, PrintWriter}
import scala.util.control.Breaks
/**
 *AST-1数据预处理 ELT AST-1
 */
class cat2txt_ast1{
  /**
   * 预处理参考星表  ELT reference catalogues
   * @param input input path
   * @param output output path
   * @param flag
   * @param cun_k 计算的层级 healpix level of calculation
   * @param bian_k 根据误差半径选取的边缘块层级 Healpix level for boundary blocks selected according to the error radius
   */
  def filewrSample(input:String,output:String,flag:Int,cun_k:Int,bian_k:Int): Unit ={
    val fits: Fits = new Fits(input)
    val bt: BinaryTableHDU= fits.getHDU(2).asInstanceOf[BinaryTableHDU]
    val maxsize = bt.getNRows
    val d:Double = math.Pi/180

    //获取时间 Extracting time
    val hdu1: BinaryTableHDU = fits.getHDU(1).asInstanceOf[BinaryTableHDU]
    val str = hdu1.getData.getColumn(0).asInstanceOf[Array[AnyRef]]
    val a: Array[String] = str(0).asInstanceOf[Array[String]]
    val data: String = a(52).substring(11,21)
    val time:String = a(52).substring(22,35)
    val data_time = data+ "T"+time

    //存储块 healpix block
    val hb_cun: HealpixBase = new HealpixBase(Math.pow(2,cun_k).toLong, Scheme.NESTED)
    //边缘块 boundary block
    val hb_bian: HealpixBase = new HealpixBase(Math.pow(2,bian_k).toLong, Scheme.NESTED)

    val bos = new BufferedOutputStream(new FileOutputStream(output, true))
    var k = 0
    for(i <- 0 until maxsize) {
      val chijing = bt.getRow(i)(4).asInstanceOf[Array[Double]]
      val chiwei = bt.getRow(i)(5).asInstanceOf[Array[Double]]
      val point: Pointing = new Pointing((90 - chiwei(0)) * d, chijing(0) * d)
      val healpix_cun = hb_cun.ang2pix(point)
      val healpix_small = hb_bian.ang2pix(point)

      val mag = bt.getRow(i)(10).asInstanceOf[Array[Float]](0)
      val magerr = bt.getRow(i)(11).asInstanceOf[Array[Float]](0)

      val n_neighbours: Array[Long] = hb_bian.neighbours(healpix_small)
      var neighbour_pix: Int= 0
      val bijiao_pre = Integer.toBinaryString(healpix_cun.toInt) //二进制
      val len = input.split("\\\\")(input.split("\\\\").length-1).length
      val source = input.split("\\\\")(input.split("\\\\").length-1).substring(0,len-4)
      val count: Array[Int] = Array(0, 0, 0, 0,0 ,0,0,0)

      for(m <- List(0,2,4,6)){
        val length = Integer.toBinaryString(n_neighbours(m).toInt).length
        val bijiao_next = Integer.toBinaryString(n_neighbours(m).toInt).substring(0,length-8)
        if(bijiao_next != bijiao_pre){
          count(m) = 1
        }
      }
      for(j <- List(0,2,4,6)){
        if(count(j) == 1){
          val len = Integer.toBinaryString(n_neighbours(j).toInt).length
          neighbour_pix = Integer.parseInt(Integer.toBinaryString(n_neighbours(j).toInt).substring(0,len-8),2)
          val str = flag+" "+source+"_"+k+" "+chijing(0) + " " + chiwei(0) + " "+ mag+" "+magerr+" "+neighbour_pix + " " + "-" +" "+ data_time+" " +0 +" "+ 0
          bos.write(str.getBytes(), 0, str.getBytes.length)
          bos.write("\r\n".getBytes())
          k = k+1
        }
      }
      //冗余存储 redundant storage
      val str = flag+" "+source+"_"+k+" "+chijing(0) + " " + chiwei(0) + " "+ mag+" "+magerr+" "+healpix_cun + " " + "-" +" "+ data_time+" " +0 +" "+ 0
      bos.write(str.getBytes(), 0, str.getBytes.length)
      bos.write("\r\n".getBytes())
      k = k+1
    }
    bos.flush()
    bos.close()
  }

  /**
   * 其他星表 other catalogues
   * @param input
   * @param output
   * @param flag
   * @param cun_k
   * @param bian_k
   */
  def filewr(input:String, output:String,flag:Int,cun_k:Int,bian_k:Int): Unit ={
    val fits: Fits = new Fits(input)
    val bt: BinaryTableHDU= fits.getHDU(2).asInstanceOf[BinaryTableHDU]
    val maxsize = bt.getNRows
    val d:Double = math.Pi/180

    val hdu1: BinaryTableHDU = fits.getHDU(1).asInstanceOf[BinaryTableHDU]
    val str = hdu1.getData.getColumn(0).asInstanceOf[Array[AnyRef]]
    val a: Array[String] = str(0).asInstanceOf[Array[String]]
    val data: String = a(52).substring(11,21)
    val time:String = a(52).substring(22,35)
    val data_time = data+"T"+time

    val hb_cun: HealpixBase = new HealpixBase(Math.pow(2,cun_k).toLong, Scheme.NESTED) //存储块
    val hb_bian: HealpixBase = new HealpixBase(Math.pow(2,bian_k).toLong, Scheme.NESTED) //计算快--计算边缘块

    val bos = new BufferedOutputStream(new FileOutputStream(output, true))
    var k = 0

    for(i <- 0 until maxsize) {
      val chijing = bt.getRow(i)(4).asInstanceOf[Array[Double]]
      val chiwei = bt.getRow(i)(5).asInstanceOf[Array[Double]]
      val point: Pointing = new Pointing((90 - chiwei(0)) * d, chijing(0) * d)
      val healpix_cun = hb_cun.ang2pix(point) //存储块pix
      val healpix_small = hb_bian.ang2pix(point) //计算快pix

      val mag = bt.getRow(i)(10).asInstanceOf[Array[Float]](0)
      val magerr = bt.getRow(i)(11).asInstanceOf[Array[Float]](0)


      val n_neighbours: Array[Long] = hb_bian.neighbours(healpix_small)
      var neighbour_pix: Int= 0

      val bijiao_pre = Integer.toBinaryString(healpix_cun.toInt) //二进制
      val len = input.split("\\\\")(input.split("\\\\").length-1).length
      val source = input.split("\\\\")(input.split("\\\\").length-1).substring(0,len-4)
      val count: Array[Int] = Array(0, 0, 0, 0,0 ,0,0,0)
      for(m <- List(0,2,4,6)){
        val length = Integer.toBinaryString(n_neighbours(m).toInt).length
        val bijiao_next = Integer.toBinaryString(n_neighbours(m).toInt).substring(0,length-8)
        if(bijiao_next != bijiao_pre){
          count(m) = 1
        }
      }

      var neighbour_pixStr = ""
      val loop = new Breaks
      loop.breakable {
        for(j <- List(0,2,4,6)){
          if(count(j) == 1){
            val len = Integer.toBinaryString(n_neighbours(j).toInt).length
            val pix_String = Integer.parseInt(Integer.toBinaryString(n_neighbours(j).toInt).substring(0,len - 8),2).toString
            if(neighbour_pixStr.length ==0) neighbour_pixStr = pix_String
            else neighbour_pixStr = neighbour_pixStr + "," + pix_String
          }
          if(neighbour_pixStr.length != 0){
            //file.print(flag+" "+source+"_"+k+" "+ chijing(0) + " " + chiwei(0)+ " "+ mag+" "+magerr+" "+ healpix_cun + " " + neighbour_pixStr + " "  +data_time+" " +0 +" "+ 0+ "\n")
            val str = flag+" "+source+"_"+k+" "+ chijing(0) + " " + chiwei(0)+ " "+ mag+" "+magerr+" "+ healpix_cun + " " + neighbour_pixStr + " "  +data_time+" " +0 +" "+ 0
            bos.write(str.getBytes(), 0, str.getBytes.length)
            bos.write("\r\n".getBytes())

            k = k+1
            loop.break()
          }
          if(neighbour_pixStr.length == 0){
            //file.print(flag+" "+source+"_"+k+" "+ chijing(0) + " " + chiwei(0) + " "+ mag+" "+magerr+" "+healpix_cun + " " + "-" + " "  +data_time+" " +0 +" "+ 0+"\n")
            val str = flag+" "+source+"_"+k+" "+ chijing(0) + " " + chiwei(0) + " "+ mag+" "+magerr+" "+healpix_cun + " " + "-" + " "  +data_time+" " +0 +" "+ 0
            bos.write(str.getBytes(), 0, str.getBytes.length)
            bos.write("\r\n".getBytes())
            k = k+1
            loop.break()
          }
        }
      }
    }
    bos.flush()
    bos.close()
  }
}

