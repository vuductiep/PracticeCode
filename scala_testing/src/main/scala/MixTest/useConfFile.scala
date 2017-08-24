package MixTest


import com.typesafe.config.{Config, ConfigFactory}

object confTest{
    def main (args : Array[String] ) : Unit = {
        val confFile = "MapMatching\\MapMatching.conf1"
        try {
            val conf : Config = ConfigFactory.load(confFile)
            println( conf.getString("Conf.Mysql.url") )
            println( conf.getString("Conf.postgreSQL.url") )
        } catch {
            case ex : com.typesafe.config.ConfigException =>
                println("Missing Configuration " +ex )
        }
    }
}