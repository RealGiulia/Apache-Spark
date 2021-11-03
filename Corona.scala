package com.sundogsoftware.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Corona {
//  Criando um DataSet (é necessário tipar as colunas!).
  case class numberOfDeathsDay(extract_date: String, specimen_date: String, Number_tested: Int, Number_confirmed: Int, Number_hospitalized: Int, Number_deaths: Int)

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Criando um SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // Convertendo o arquivo par um DataSet, seguindo as especificações definidas na case class
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/coronainfo.csv")
      .as[numberOfDeathsDay]

    //Como desejamos saber qual dia teve o maior número de mortes, vamos fazer as operações necessárias no DataSet.
    val deathsPerDay = ds.select("specimen_date", "Number_deaths")
    val deaths = deathsPerDay.filter("Number_deaths >  800")
    val recordDeaths = deaths.groupBy("specimen_date").max("Number_deaths")
    recordDeaths.show()

    spark.stop()

  }

}
