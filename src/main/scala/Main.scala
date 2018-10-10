
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class Flight(YEAR: String, MONTH: String, DAY: String, DAY_OF_WEEK: String, AIRLINE: String, FLIGHT_NUMBER: String, TAIL_NUMBER: String, ORIGIN_AIRPORT: String, DESTINATION_AIRPORT: String, SCHEDULED_DEPARTURE: String, DEPARTURE_TIME: String, DEPARTURE_DELAY: String, TAXI_OUT: String, WHEELS_OFF: String, SCHEDULED_TIME: String, ELAPSED_TIME: String, AIR_TIME: String, DISTANCE: String, WHEELS_ON: String, TAXI_IN: String, SCHEDULED_ARRIVAL: String, ARRIVAL_TIME: String, ARRIVAL_DELAY: String, DIVERTED: String, CANCELLED: String, CANCELLATION_REASON: String, AIR_SYSTEM_DELAY: String, SECURITY_DELAY: String, AIRLINE_DELAY: String, LATE_AIRCRAFT_DELAY: String, WEATHER_DELAY: String) extends Serializable {}
case class Airport(IATA_CODE: String, AIRPORT: String, CITY: String, STATE: String, COUNTRY: String, LATITUDE: String, LONGITUDE: String)

object FlightAnalyticsWithGraphx extends App {

  override def main(arg: Array[String]): Unit = {

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    import spark.implicits._
    // # File location and type
    val file_location = "file:///home/suket/Documents/FlightData/flights.csv"
    val file_type = "csv"

    // # CSV options
    val infer_schema = "false"
    val first_row_is_header = "true"
    val delimiter = ","

    // # The applied options are for CSV files. For other file types, these will be ignored.
    val flights = spark.read.format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(file_location).as[Flight].rdd

    val file_location2 = "file:///home/suket/Documents/FlightData/airports.csv"

    val airports = spark.read.format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(file_location2).as[Airport].rdd

    val airportVertex = airports.map(airport => (airport.IATA_CODE.hashCode.toLong, airport))
    val flightsEdge = flights.map(flight => Edge(flight.ORIGIN_AIRPORT.hashCode.toLong, flight.DESTINATION_AIRPORT.hashCode.toLong, flight))

    val flightsEdgeShort = sc.parallelize(flightsEdge.take(1000))
    val defaultAirport = Airport("NA", "Not Defined", "No City", "No State", "No Country", "NA", "NA")
    val airlineGraph = Graph(airportVertex, flightsEdgeShort, defaultAirport)

    //Query 1 - Find the total number of airports
    val numairports = airlineGraph.vertices.count
    println()
    println("Number of Airports = " + numairports)

    //Query 2 - Calculate the total number of routes?
    val numroutes = airlineGraph.edges.count
    println()
    println("Total Number of Flights = " + numroutes)

    //Query 3 - Count those flights with distances more than 1000 miles
    val longFlights = airlineGraph.edges.filter { edge => edge.attr.DISTANCE.toDouble > 1000 }.count
    println()
    println("Number of Flights with distances more than 1000 miles = " + longFlights)

    //Similarly write Scala code for the below queries
    //Query 4 - Sort and print the longest 5 routes
    println()
    airlineGraph.convertToCanonicalEdges().triplets.sortBy(_.attr.DISTANCE.toDouble, ascending = false).map(triplet =>

      "Distance " + triplet.attr.DISTANCE + " from " + triplet.srcAttr.AIRPORT + " to " + triplet.dstAttr.AIRPORT + ".").take(10).foreach(println)

    //val longest_5_Routes :Array[org.apache.spark.graphx.Edge[Flight]] = airlineGraph.edges.sortBy(_.attr.DISTANCE.toDouble , ascending=false).take(5)
    //longest_5_Routes.foreach{ f => println("Flight Number = " + f.attr.FLIGHT_NUMBER + "  Distance = " +  f.attr.DISTANCE + " from " + f.attr.ORIGIN_AIRPORT + " to " + f.attr.DESTINATION_AIRPORT)}

    //Query 5 - Display highest degree vertices for incoming and outgoing flights of airports
    val HighestIndegreeAirport = airlineGraph.inDegrees.collect.sortWith(_._2 > _._2)(0)
    val HighestOutdegreeAirport = airlineGraph.outDegrees.collect.sortWith(_._2 > _._2)(0)

    //Query 6 - Get the airport name with IDs 78531 and 76304
    val RequiredAirports = airlineGraph.vertices.filter { case (vertexId, airport) => vertexId == 78531 || vertexId == 76304 }.collect
    println()
    println("Get the airport name with IDs 78531 and 76304 ")
    RequiredAirports.map(airport => "Airport Id = " + airport._1 + " Airport Name = " + airport._2.AIRPORT).foreach(println)
    println()

    //Query 7 - Find the airport with the highest incoming flights

    val AirportWithHighestIncomingFlights = airlineGraph.vertices.filter { case (vertexId, airport) => vertexId == HighestIndegreeAirport._1 }.collect
    println(" Find the airport with the highest incoming flights")
    println(AirportWithHighestIncomingFlights(0)._2.AIRPORT + " with " + HighestIndegreeAirport._2 + " incoming flights.")
    println()

    //Query 8 - Find the airport with the highest outgoing flights

    val AirportWithHighestOutgoingFlights = airlineGraph.vertices.filter { case (vertexId, airport) => vertexId == HighestOutdegreeAirport._1 }.collect
    println(" Find the airport with the highest outgoing flights")
    println(AirportWithHighestOutgoingFlights(0)._2.AIRPORT + " with " + HighestOutdegreeAirport._2 + " outgoing flights.")
    println()

    //Query 9 - Find the most important airports according to PageRank

    val ranks = airlineGraph.pageRank(0.01).vertices
    val temp = ranks.join(airportVertex)

    //Query 10 - Sort the airports by ranking

    // sort by ranking
    val temp2 = temp.sortBy(_._2._1, false)
    val impAirports = temp2.map(_._2._2.AIRPORT)

    //Query 11 - Display the most important airports

    println("Find the most important airports according to PageRank")
    impAirports.take(10).foreach(println)
    println()

    //Query 12 - Find the Routes with the lowest flight costs
    //Query 13 - Find airports and their lowest flight costs
    //Query 14 - Display airport codes along with sorted lowest flight costs

    sc.stop()

  }

}