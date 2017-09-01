//Sed works only in linux os

import java.io.{File, PrintWriter}
import sys.process._

import scala.io.Source

object ProcessText2 {
    def main(args: Array[String]) : Unit = {
        val pathfile = "E:\\project\\Mapmatching\\segmentindex.sql"
        //        val pathfile = "E:\\project\\Mapmatching\\test.txt"
        val file = new File(pathfile)
        val bufferedSource = Source.fromFile(pathfile)
        val pw = new PrintWriter(new File("processed_" + file.getName + ".csv" ))

        println("Start converting")
        pw.write("segment_id, link_id, vtx_1, vty_1, vtx_2, vty_2, mesh1, mesh2\n")
        var count = 0
        //        for ( line <- bufferedSource.getLines() ) {
        for ( line <- bufferedSource.getLines().drop(38) ) {
            count = count + 1
            val newLine = line.replace("INSERT INTO `vertexdata` VALUES (", "").replace(");", "\n").replace("'", "")
            pw.write(newLine)
        }
        bufferedSource.close()
        pw.close()
        printf("Process %d lines done \n", count)
//        Seq("sed","-i","$ d","processed_" + file.getName + ".csv")!
    }
}

