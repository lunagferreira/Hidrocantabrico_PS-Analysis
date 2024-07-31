```scala
// Databricks notebook source
// DBTITLE 1,Libraries and Input Files
// Importing the required libraries
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Initializing Spark session
val spark = SparkSession.builder().appName("HIDROCANTABRICO_PS").getOrCreate()

// Reading the format file to get a schema
val formatContent = spark.read.textFile("dbfs:/FileStore/EDP/formatos/FormatoFicheroSuministros.txt").collect()

// List of files in "HIDROCANTABRICO_PS"
val dataFiles = dbutils.fs.ls("dbfs:/FileStore/EDP/HIDROCANTABRICO_PS").map(_.path)

// DBTITLE 1,Schema Fields
// Extracting column names and lengths 
val schemaFields = formatContent.map(line => {
  val parts = line.split("\\(")
  val columnName = parts(0).trim
  val columnLength = parts(1).replace(")", "").toInt
  (columnName, columnLength)
})

schemaFields.foreach(println)

// DBTITLE 1,Schema
// Function to parse a line into columns based on the format
def parseLine(line: String): Row = {
  var values = Seq[String]() 
  var position = 0
  
  // Iterating over each column definition (name, lenght) in the schema
  for ((colName, length) <- schemaFields) {
    val value = line.substring(position, Math.min(position + length, line.length)).trim // Substring for this column

    values = values :+ value // Append to list of values
    position += length // Next position
  }
  Row(values: _*) // Getting a row from the list of values
}

// Defining the schema for the DF using the extracted schema fields
val schema = StructType(schemaFields.map { case (colName, length) => StructField(colName, StringType, nullable = true) })

// DBTITLE 1,Parsing into DF
// Function to read and parse a file into a DF
def readFileToDataFrame(filePath: String): DataFrame = {
  val fileContent = spark.read.textFile(filePath) // Reading the file content
  
  val rows = fileContent.rdd.map(parseLine) // Parsing each line of the file into a Row
  val df = spark.createDataFrame(rows, schema)
  
  println(s"$filePath")
  df.show(2, 54, true)
  df
}

// Reading each file and creating a DF
val dataFrames = dataFiles.map(readFileToDataFrame)
