import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.If
import scala.io.StdIn.readLine

object p1 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    /*print("BASIC or ADMIN? ")
    val BasicOrAdmin = readLine()

    val z = "Basic"
    val x = "Admin"

    if (BasicOrAdmin == z) {
      println("Enter Basic Password:")
      val BasicPassword = readLine()
      if (BasicPassword == "imbasic") {
        println("Welcome to Basic User. To View Six Queries enter yes. To exit, enter no")
        val yesorno = readLine()
        if (yesorno == "yes"){
          println("Here you go!")
        }

      }
    }

    else if (BasicOrAdmin == x) {
      println("Enter Admin Password:")
      val AdminPassword = readLine()
      if (AdminPassword == "superuser") {
        println("Welcome Admin. You can create, read, update, or delete tables. To View Six Queries enter yes. To exit, enter no")
        val yesorno = readLine()
        if (yesorno == "yes"){
          println("Here you go!")
        }

      }
    }

    else println("Try again")*/





    //spark.sql("DROP TABLE Storm_Fatalities")
    //spark.sql("CREATE TABLE Storm_Fatalities (FAT_YEARMONTH string, FAT_DAY string, FAT_TIME string, FATALITY_ID string, EVENT_ID string, FATALITY_TYPE string, FATALITY_DATE string, FATALITY_AGE int, FATALITY_SEX string, FATALITY_LOCATION string, EVENT_YEARMONTH string) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'Input/StormEvents_fatalities-2019.txt' INTO TABLE Storm_Fatalities")
    //spark.sql("LOAD DATA LOCAL INPATH 'Input/StormEvents_fatalities-2020.txt' INTO TABLE Storm_Fatalities")
    //spark.sql("DROP TABLE Storm_locations")
    //spark.sql("CREATE TABLE Storm_locations (YEARMONTH String,EPISODE_ID String ,EVENT_ID String ,LOCATION_INDEX String, RANGE String, AZIMUTH String,LOCATION String,LATITUDE String,LONGITUDE String, LAT2 STRING,LON2 STRING) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'Input/StormEvents_locations.txt' INTO TABLE Storm_locations")

   /*Scenario 1
        println("Scenario 1")
        println("Number of Fatalities from 2019/2020: ")
       // = spark.sql("SELECT COUNT(FATALITY_ID) AS 2019_Fatalities FROM Storm_Fatalities" )
      val a = {
        spark.sql("SELECT COUNT(FATALITY_ID) AS Total_Fatalities FROM Storm_Fatalities")
      }
      a.show()*/

    //Scenario 2
    println("Scenario 2")
    println("Male vs Female: ")
    val b = spark.sql("SELECT COUNT(*) AS Male_vs_Female FROM Storm_Fatalities WHERE FATALITY_SEX='\"M\"' UNION SELECT COUNT(*) FROM Storm_Fatalities WHERE FATALITY_SEX = '\"F\"'")

    b.show()

    /*Scenario 3
    println("Scenario 3")
    println("Vulnerable location: ")
    val c = {
      spark.sql("SELECT (FATALITY_LOCATION) FROM Storm_Fatalities GROUP BY FATALITY_LOCATION ORDER BY COUNT (*) DESC LIMIT 1 ")
    }
    c.show()

    val d = {
      spark.sql("SELECT COUNT (FATALITY_LOCATION) FROM Storm_Fatalities WHERE FATALITY_LOCATION = '\"Vehicle/Towed Trailer\"' ")
    }
    d.show()*/

    /*scenario 4
    println("Scenario 4")
    println("Average age: ")
    val e = {
      spark.sql("SELECT ROUND(AVG(FATALITY_AGE)) AS Average_AGE_of_DEATH FROM Storm_Fatalities")
    }
    e.show()*/

    /*scenario 5
    println("Scenario 5")
    println("Recurring Month from 2020: ")
    val f = {
      spark.sql("SELECT (YEARMONTH) FROM Storm_locations GROUP BY YEARMONTH ORDER BY COUNT (*) DESC LIMIT 1 ")
    }
    f.show()*/

    /*scenario 6
    println("Scenario 6")
    println("Future Scenario: ")
    println("Where most storms will occur by Latitude & Longitude: ")
    val g = {
      spark.sql("(SELECT (LATITUDE) AS Latitude_Longitude FROM Storm_locations GROUP BY LATITUDE ORDER BY COUNT (*) DESC LIMIT 1) UNION ALL (SELECT (LONGITUDE) FROM Storm_locations GROUP BY LONGITUDE ORDER BY COUNT (*) DESC LIMIT 1)")

    }
    g.show()
    println("NEWPORT NEWS, Virginia")*/

  }
}


//Bueno come dice la gente, con fuerza si se pudo
