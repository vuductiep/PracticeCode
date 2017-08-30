import java.io.{File, PrintWriter}

import scala.io.Source

object ProcessText {
    def main(args: Array[String]) : Unit = {
        val pathfile = "E:\\project\\Mapmatching\\segmentindex.sql"
//        val pathfile = "E:\\project\\Mapmatching\\test.txt"
        val file = new File(pathfile)
        val bufferedSource = Source.fromFile(pathfile)
        val pw = new PrintWriter(new File("E:\\project\\Mapmatching\\processed_" + file.getName ))

        var count = 0
//        for ( line <- bufferedSource.getLines() ) {
        for ( line <- bufferedSource.getLines().drop(44) ) {
            count = count + 1
            val newLine = line.replace(";", ",\n").replace("INSERT INTO `vertexdata` VALUES", "")
            pw.write(newLine)
        }
        bufferedSource.close()
        pw.close()
        printf("Process %d lines done ", count)
    }
}