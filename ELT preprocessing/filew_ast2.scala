package com.quan.catTotxt
import java.io.File

object filew_ast2 {
  def main(args: Array[String]): Unit = {
    val cat2txt = new cat2txt_ast2()
        var flag = 1
        val f = new File("..\\Data\\ELT_data\\ast1\\Tess004\\") //获取路径
        val fa: Array[File] = f.listFiles
        for (i <- 0 until fa.size) {
          val fs: File = fa(i)
          val catname = fs.getName
          val txtname = catname.replace("cat","txt")
          cat2txt.newCat2other(
            "..\\Data\\ELT_data\\ast1\\Tess004\\" + catname,
            "..\\Data\\ELT_data\\ast1\\Tess004_out\\" + txtname,
            flag,10,13)
          flag = flag+1
        }
    cat2txt.newCat2sample("..\\Data\\ELT_data\\ast1\\Tess004\\AST3II-Tess004_000001_000957.cat",
      "..\\Data\\ELT_data\\ast1\\Tess004_out\\AST3II-Tess004_000001_000957.txt",
      1,10,13)
  }
}
