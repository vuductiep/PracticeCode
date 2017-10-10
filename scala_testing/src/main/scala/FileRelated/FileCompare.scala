import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer

import scala.io.Source

object FileCompare {
    def main(args: Array[String]): Unit = {
        val pfile1 = "C:\\Users\\tiep\\java-workspace\\Mapmatching\\matching\\resultMapping 2017-05-12 11-05-52\\part-00000"
        val pfile2 = "E:\\project\\Mapmatching\\matching\\java120000data.csv"

        val file1 = new File(pfile1)
        val file2 = new File(pfile2)

        val bufSource1 = Source.fromFile(file1)
        val bufSource2 = Source.fromFile(file2)

        val pw = new PrintWriter(new File("C:\\Users\\tiep\\java-workspace\\Mapmatching\\matching\\compare_" + file1.getName + "_" + file2.getName))

        var count = 0
        val list1 = ListBuffer[String]()
        val list2 = ListBuffer[String]()
        for (line <- bufSource1.getLines()) {
            list1 += line.trim
        }
        for (line <- bufSource2.getLines()) {
            list2 += line.trim
        }

        val res = pairUp(list1.toList, list2.toList)

        res.map(line => {
            pw.write(line._1 + " -> " + line._2 + "\n")
        })
        pw.close()
    }

    def pairUp(list1: List[String], list2: List[String]): List[(String, String)] = {
        val g1 = list1.groupBy(_.hashCode).toList
        val g2 = list2.groupBy(_.hashCode)
        g1.flatMap{ case (k,as) => g2.get(k).toList.flatMap(as zip _) }
    }

}
