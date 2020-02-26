package booster

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types._
import java.util.TreeMap

case class Visit(date: String, country: String, revenue: Float, ip: Long)

object DataFrameApp {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.conf

    val uvPath = sc.get("spark.booster.uv.path")
    val geoipBlocksPath = sc.get("spark.booster.geoip.blocks.path")
    val geoipLocationPath = sc.get("spark.booster.geoip.location.path")
    val outputPath = sc.get("spark.booster.output.path")

    val hc = spark.sparkContext.hadoopConfiguration
    hc.set("fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val uv = spark
      .read
      .format("csv")
      .option("header", true)
      .load(uvPath)


    val visitEncoder = Encoders.product[Visit]


    val parsedUv = uv.map(r => Visit(
      r.getString(2).take(4),
      r.getString(5),
      r.getString(3).toFloat,
      ipToLong(r.getString(0)),
    ))(visitEncoder)


    parsedUv.show(3)

    def blocksCsv = { StructType(Array(
      StructField("startIpNum", LongType, true),
      StructField("endIpNum", LongType, true),
      StructField("locId", LongType, true)))
    }

    val geoipBlocksMap = spark
      .read
      .format("csv")
      .schema(blocksCsv)
      .option("header", true)
      .load(geoipBlocksPath)
      .collect()
      .collect {
        case Row(startIpNum: Long, endIpNum: Long, locId: Long) => (startIpNum, locId)
      }
      .toMap

    val geoipBlocksTreeMap = scala.collection.immutable.TreeMap(geoipBlocksMap.toArray:_*)
    val locIdByIp = spark.sparkContext.broadcast(geoipBlocksTreeMap)
    val locIdSearch = udf((ip: Long) => locIdByIp.value.to(ip).last._2)

    val parsedUvWithLocId = parsedUv
      .withColumn("locId", locIdSearch(col("ip")))


    def locationsCsv = { StructType(Array(
      StructField("locId", LongType, true),
      StructField("country", StringType, true),
      StructField("region", StringType, true),
      StructField("city", StringType, true),
      StructField("postalCode", LongType, true),
      StructField("latitude", FloatType, true),
      StructField("longitude", FloatType, true),
      StructField("metroCode", IntegerType, true),
      StructField("areaCode", IntegerType, true)))
    }

    val geoipLocationMap = spark
      .read
      .format("csv")
      .schema(locationsCsv)
      .option("header", true)
      .load(geoipLocationPath)
      .collect()
      .collect {
        case Row(locId: Long, country: String, region: String, city: String, postalCode: Long, latitude: Float, longitude: Float, metroCode: Int, areaCode: Int)
        => (locId, city)
      }
      .toMap

    val cityByLocId = spark.sparkContext.broadcast(geoipLocationMap)
    val citySearch = udf((locId: Long) => cityByLocId.value.get(locId))

    val parsedUvWithCity = parsedUvWithLocId
      .withColumn("city", citySearch(col("locId")))
      .filter(col("city").isNotNull)
      .select("date", "country", "city", "revenue")
      .map{
        case Row(date: String, country: String, city: String, revenue: Float) => (date, country, city, revenue)
      }(Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.scalaFloat)).rdd

    parsedUvWithCity.groupBy { case (year, country, city, revenue) => (year, country, city) }
      .mapValues(_.map { case (year, country, city, revenue) => revenue }.sum)
      .map { case ((year, country, city), revenue) => (year, (country, city, revenue)) }
      .groupBy { case (year, (country, city, revenue)) => year }
      .mapValues(_.toList.map { case (year, (country, city, revenue)) => (country, city, revenue) }.sortBy { case (country, city, revenue) => revenue }.reverse.take(3))
      .sortBy { case (year, List(country, city, revenue)) => year }
      .take(10)
      .foreach(println)

  }

  private def ipToLong(ipAddress: String): Long = {
    val ipAddressInArray: Array[String] = ipAddress.split("\\.")
    var result: Long = 0
    for (i <- 0 until ipAddressInArray.length) {
      val power: Int = 3 - i
      val ip: Int = java.lang.Integer.parseInt(ipAddressInArray(i))
      result += (ip * Math.pow(256, power)).toLong
    }
    result
  }
}
