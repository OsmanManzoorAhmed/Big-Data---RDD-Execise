package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.math._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph


/** GeoProcessor provides functionalites to 
* process country/city/location data.
* We are using data from http://download.geonames.org/export/dump/
* which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
*
* @param spark reference to SparkSession 
* @param filePath path to file that should be modified
*/
class GeoProcessor(spark: SparkSession, filePath:String) {

    //read the file and create an RDD
    //DO NOT EDIT
    val file = spark.sparkContext.textFile(filePath)

    /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
    def filterData(data: RDD[String]): RDD[Array[String]] = {
        /* hint: you can first split each line into an array.
        * Columns are separated by tab ('\t') character.
        * Finally you should take the appropriate fields.
        * Function zipWithIndex might be useful.
        */

        val filteredData    = data.map(x => x.split('\n'))
        val filteredColumns = filteredData.map(x => x(0).trim.split('\t')).map(array => Array(array(1),array(8),array(16)))
        //val filteredColumns = data.map(x => x.split('\t')).map(array => Array(array(1),array(8),array(16)))
        //val indexes  = Array(1,8,16)
        //val filteredColumns = data.map(x => x.split('\t')).map(y => y.zipWithIndex.filter(r => indexes.contains(r._2)).map(e => e._1))


        return filteredColumns
        //val filteredData = data.flatMap(x => x.split('\t'))
        //filteredData.take(5).foreach(x => println(x.mkString(" ")))
        //return filteredData
    }


    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String,data: RDD[Array[String]]): RDD[Int] = {

      val filteredElevation = data.filter(x => x(1) == countryCode)
      val filteredElev =  filteredElevation.map(x => x(2).toInt)

      return filteredElev
    }




    /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
    def elevationAverage(data: RDD[Int]): Double = {
      //Double averageElev = data.map(())
      val sum = data.map(x => x.doubleValue()).sum()
      val count = data.count().toDouble
      val average = (sum/count)

      return average
    }

    /** mostCommonWords calculates what is the most common 
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '. 
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of 
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
    def mostCommonWords(data: RDD[Array[String]]): RDD[(String,Int)] = {

      val counts = data.flatMap(line => line(0).split(" "))
      val KeyPair = counts.map(x => (x,1))
      val KeyPairSum = KeyPair.reduceByKey((x,y) => (x+y)).sortBy(x => x._2,false)

      return KeyPairSum
    }

    /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
    def mostCommonCountry(data: RDD[Array[String]], path: String): String = {

      val countryvalue = data.map(x => x(1))
      val countryvaluepair = countryvalue.map(x => (x,1))
      val countryCount = countryvaluepair.reduceByKey((x,y) => (x+y)).sortBy(x => x._2,false)
      val countryVal = countryCount.first()._1

      val fileCountry = spark.sparkContext.textFile(path)
      val splitCountry = fileCountry.map(x => x.split(","))
      val country = splitCountry.filter(x => x(1)== countryVal).map(x => x(0))

      if(country.isEmpty())
        {
          return ""
        }
      else
        {
          return country.first()
        }
    }


//
    /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
    def hotelsInArea(lat: Double, long: Double): Int = {

      val filteredData    = file.map(x => x.split('\n'))
      val filteredColumns = filteredData.map(x => x(0).trim.split('\t')).map(array => Array(array(1),array(4),array(5)))
      val filteredHotels = filteredColumns.filter(x => x(0).toLowerCase.contains("hotel"))
      //filteredHotels.take(10).foreach(x => println(x.mkString(" ")))

      val calulateHermasian = filteredHotels.map(x => {
        val latDistance = math.toRadians(lat.toRadians - x(1).toDouble.toRadians)
        val longDistance = math.toRadians(long.toRadians - x(2).toDouble.toRadians)
        val mid = math.pow(math.sin(latDistance/2),2) + math.cos(lat.toRadians) *  math.cos(x(1).toDouble.toRadians) * math.pow(math.sin(longDistance/2),2)
        //val hermasian = 2 * 6371e3 * math.asin(math.sqrt(mid))
        val hermasian = 2 * 6371e3 * math.atan2(math.sqrt(mid),math.sqrt(1-mid))*1000
        (x(0),x(1),x(2),hermasian.toDouble)
      }
      )

      val filteredHermasian = calulateHermasian.filter(x => x._4 <= 10000.0)
      val HarmesianHotel = filteredHermasian.map(x => x._1).distinct()
      val HarmesianHotelCount = HarmesianHotel.count().toInt

      return HarmesianHotelCount
    }

    //GraphX exercises

    /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/latest/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
    def loadSocial(path: String): Graph[Int,Int] = {
      val file = spark.sparkContext.textFile(path)

      val GraphData = file.map(x => x.replace(" ",""))

      //val MidData = GraphData.flatMap(x => x.split("\n"))
      //GraphData.map()
      val GraphTrimed = GraphData.filter(x => x.matches("""\d+\|\d+"""))
      val GraphPair = GraphTrimed.map(x => x.split('|')).map(x => List(x(0),x(1)))
      val GraphPairCount = GraphPair.map(x => (x,1))
      val GraphCount = GraphPairCount.reduceByKey((x,y) => (x+y)).sortBy(_._2,false)
      //
      //GraphData.take(10).foreach(x => println(x))

      //Data for Vertex
      val Vertices = GraphTrimed.flatMap(x => x.split('|')).distinct().sortBy(x => x)

      val UniqueVertices: RDD[(VertexId, Int)] = Vertices.map(x => (x.toInt,x.toInt))
      //val UniqueVertices = Vertices.map(x => (x.toInt,x))
      //UniqueVertices.foreach(x => println(x))


      val Edges = GraphCount.map(x => Edge(x._1(0).toInt,x._1(1).toInt,x._2.toInt))
      val graphX = Graph(UniqueVertices, Edges)

      return graphX
    }

    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int,Int]): Int = {
      return graph.outDegrees.map(x => (x._1,x._2)).sortBy(x => x._2,false).first()._1.toInt
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
    def pageRankHighest(graph: Graph[Int,Int]): Int = {

      return graph.pageRank(0.0001).vertices.sortBy(x => x._2,false).first()._1.toInt
    } 
}
/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "721347"
}