package com.quan.catTotxt
import java.io.File

object filew_ast1 {
  def main(args: Array[String]): Unit = {
    val ct = new cat2txt_ast1
    var flag = 1
    val f = new File("..\\Data\\ELT_data\\ast1\\HD88500\\") //获取路径
    val fa: Array[File] = f.listFiles
    for (i <- 0 until fa.size) {
      val fs: File = fa(i)
      val catname = fs.getName
      val txtname = catname.replace("cat","txt")
      ct.filewr(
        "..\\Data\\ELT_data\\ast1\\HD88500\\" + catname,
        "..\\Data\\ELT_data\\ast1\\HD88500_out\\" + txtname,
        flag,10,13)
      flag = flag+1
    }
    ct.filewrSample("..\\Data\\ELT_data\\ast1\\HD88500\\a0507.1.cat",
      "..\\Data\\ELT_data\\ast1\\HD88500_out\\a0507.1.txt",
      1,10,13)
  }
}


