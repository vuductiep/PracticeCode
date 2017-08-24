package MixTest

import java.io.{File, PrintWriter}

import scala.io.Source

object Demo {
    def main(args: Array[String]) {
        println("Following is the content read:" )
        val pathfile = "C:\\Users\\tiep\\Documents\\arxDemo.csv"
        val file = new File(pathfile)
        val pw = new PrintWriter(new File("C:\\Users\\tiep\\java-workspace\\Mapmatching\\matching\\" + file.getName ), "euc-kr")

        val bufferedSource = Source.fromFile(pathfile, "euc-kr")
        for ( line <- bufferedSource.getLines() ) {
            var b : Array[String] = line.split(',')
            println(line)
            b.foreach(println )
        }
        bufferedSource.close()
        pw.close
    }
}

