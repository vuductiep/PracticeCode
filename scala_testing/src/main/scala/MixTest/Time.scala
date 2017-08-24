package MixTest

import java.text.SimpleDateFormat

object TimeTest {
    def main(args: Array[String]) {
        println("Testing time with scala:")


        val time = System.currentTimeMillis
        val dayTime = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")
        val str = dayTime.format(time)
        val inputDirName = "C:\\Users\\u\\Desktop\\JW\\taxi trajectory\\Korea taxi trajectory\\taxi_20161029.csv"
        val outFileName = "C:\\Users\\u\\Desktop\\JW\\taxi trajectory\\mmoutput\\mmoutput_20161029.csv"

        printf("\n ##### start ##########(%s)################## \n", str)
        printf(" ## Input Dir Name : (%s)\n", inputDirName)
        printf(" ## output Dir Name : (%s)\n", outFileName)
    }
}

