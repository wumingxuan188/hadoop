package demo

import java.io.FileOutputStream
import java.net.URL

object TestPaChong {

  def main(args: Array[String]): Unit = {
    val url = new URL("http://image.baidu.com/search/index?ct=201326592&cl=2&st=-1&lm=-1&nc=1&ie=utf-8&tn=baiduimage&ipn=r&rps=1&pv=&fm=rs1&word=2017%E6%9C%80%E5%BF%83%E7%B4%AF%E7%9A%84%E5%9B%BE%E7%89%87&hs=0&oriquery=%25E4%25BC%25A4%25E5%25BF%2583%25E5%259B%25BE%25E7%2589%2587&ofr=%25E4%25BC%25A4%25E5%25BF%2583%25E5%259B%25BE%25E7%2589%2587&sensitive=0")
    val conn = url.openConnection()
    val in = conn.getInputStream
   // val out = conn.getOutputStream
    val array = new Array[Byte](in.available())
     in.read(array);
    val out = new FileOutputStream("D:\\demo\\1.html")
    out.write(array)
    in.close()
    out.close()
  }
}
