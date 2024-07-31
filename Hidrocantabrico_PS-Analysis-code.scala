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

// Reading the Provincia code file
val codificacionContent = spark.read.textFile("dbfs:/FileStore/EDP/codificacion/CodificacionCampoProvincia.txt").collect()

// Reading the Propiedad Equipo Medida code file
val propiedadEquipoMedidaContent = spark.read.textFile("dbfs:/FileStore/EDP/codificacion/CodificacionCamposPropiedadEquipoMedida.txt").collect()

// Reading the Propiedad ICP code file
val propiedadICPContent = spark.read.textFile("dbfs:/FileStore/EDP/codificacion/CodificacionCamposPropiedadICP.txt").collect()

// Reading the Telegestionado ICP code file
val telegestionadoContent = spark.read.textFile("dbfs:/FileStore/EDP/codificacion/CodificacionCamposTelegestionado.txt").collect()

// Reading the Disponibilidad ICP code file
val disponibilidadICPContent = spark.read.textFile("dbfs:/FileStore/EDP/codificacion/CodificacionCampoDisponibilidadICP.txt").collect()

// DBTITLE 1,Schema Fields
// Extracting format names and lengths 
val schemaFields = formatContent.map(line => {
  val parts = line.split("\\(")
  val columnName = parts(0).trim
  val columnLength = parts(1).replace(")", "").toInt
  (columnName, columnLength)
})

// Keeping only the first occurrence of "Provincia"
val seen = scala.collection.mutable.Set[String]()
val uniqueSchemaFields = schemaFields.filter { case (colName, _) =>
  if (seen.contains(colName)) {
    false
  } else {
    seen.add(colName)
    true
  }
}
uniqueSchemaFields.foreach(println)

// DBTITLE 1,Extracting Codes and Names
// Extracting Provincia codes and names and mapping
val provinciaMap = codificacionContent.map(line => {
  val code = line.substring(0, 2).trim
  val name = line.substring(2).trim
  (code, name)
}).toMap

// Broadcasting the Provincia map to make it available on all nodes
val provinciaBroadcast = spark.sparkContext.broadcast(provinciaMap)

// Extracting Propiedad Equipo Medida codes and names and mapping
val propiedadEquipoMedidaMap = propiedadEquipoMedidaContent.map(line => {
  val parts = line.split("-")
  val code = parts(0).trim
  val name = parts(1).trim
  (code, name)
}).toMap

// Broadcasting the Propiedad Equipo Medida map to make it available on all nodes
val propiedadEquipoMedidaBroadcast = spark.sparkContext.broadcast(propiedadEquipoMedidaMap)

// Extracting Propiedad ICP codes and names and mapping
val propiedadICPMap = propiedadICPContent.map(line => {
  val parts = line.split("-")
  val code = parts(0).trim
  val name = parts(1).trim
  (code, name)
}).toMap

// Broadcasting the Propiedad ICP map to make it available on all nodes
val propiedadICPBroadcast = spark.sparkContext.broadcast(propiedadICPMap)

// Extracting Telegestionado codes and names and mapping
val telegestionadoMap = telegestionadoContent.map(line => {
  val parts = line.split("-")
  val code = parts(0).trim
  val name = parts(1).trim
  (code, name)
}).toMap

// Broadcasting the Telegestionado map to make it available on all nodes
val telegestionadoBroadcast = spark.sparkContext.broadcast(telegestionadoMap)

// Extracting Disponibilidad ICP codes and names and mapping
val disponibilidadICPMap = disponibilidadICPContent.map(line => {
  val parts = line.split("-")
  val code = parts(0).trim
  val name = parts(1).trim
  (code, name)
}).toMap

// Broadcasting the Disponibilidad ICP map to make it available on all nodes
val disponibilidadICPBroadcast = spark.sparkContext.broadcast(disponibilidadICPMap)

// DBTITLE 1,Schema
// Function to parse a line into columns based on the format
def parseLine(line: String): Row = {
  var values = Seq[String]() 
  var position = 0
  
  // Iterating over each column definition (name, lenght) in the schema
  for ((colName, length) <- uniqueSchemaFields) {
    val value = line.substring(position, Math.min(position + length, line.length)).trim // Substring for this column

    values = values :+ value // Append to list of values
    position += length // Next position
  }
  Row(values: _*) // Getting a row from the list of values
}

// Defining the schema for the DF using the extracted schema fields
val schema = StructType(uniqueSchemaFields.map { case (colName, length) => StructField(colName, StringType, nullable = true) })

// DBTITLE 1,Parsing into DF
// Function to read and parse a file into a DF
def readFileToDataFrame(filePath: String): DataFrame = {
  val fileContent = spark.read.textFile(filePath) // Reading the file content
  
  val rows = fileContent.rdd.map(parseLine) // Parsing each line of the file into a Row
  val df = spark.createDataFrame(rows, schema)

  display(df)
  df
}

// Reading each file and creating a DF
val dataFrames = dataFiles.map(readFileToDataFrame)

// DBTITLE 1,Replace Provincia
// Function to replace Provincia codes with names
def replaceProvincia(df: DataFrame): DataFrame = {
  val replaceProvinciaUDF = udf((code: String) => provinciaBroadcast.value.getOrElse(code, code))
  df.withColumn("Provincia", replaceProvinciaUDF(col("Provincia")))
}

// Applying the replacement to each DF
val updatedDataFrames = dataFiles.map(readFileToDataFrame).map(replaceProvincia)

updatedDataFrames.foreach { df =>
  display(df)
}

// DBTITLE 1,Replace Propiedad Equipo Medida
// Function to replace Propiedad Equipo Medida codes with names
def replacePropiedadEquipoMedida(df: DataFrame): DataFrame = {
  val replacePropiedadEquipoMedidaUDF = udf((code: String) => propiedadEquipoMedidaBroadcast.value.getOrElse(code, code))
  df.withColumn("Propiedad Equipo Medida", replacePropiedadEquipoMedidaUDF(col("Propiedad Equipo Medida")))
}

// Applying the replacement to each DF
val fullyUpdatedDataFrames = updatedDataFrames.map(replacePropiedadEquipoMedida)

fullyUpdatedDataFrames.foreach { df =>
  display(df)
}

// DBTITLE 1,Replace Propiedad ICP
// Function to replace Propiedad ICP codes with names
def replacePropiedadICP(df: DataFrame): DataFrame = {
  val replacePropiedadICPUDF = udf((code: String) => propiedadICPBroadcast.value.getOrElse(code, code))
  df.withColumn("Propiedad ICP", replacePropiedadICPUDF(col("Propiedad ICP")))
}

// Applying the replacement to each DF
val fullyUpdatedDataFramesWithICP = fullyUpdatedDataFrames.map(replacePropiedadICP)

fullyUpdatedDataFramesWithICP.foreach { df =>
  display(df)
}

// DBTITLE 1,Replace Telegestionado
// Function to replace Telegestionado codes with names
def replaceTelegestionado(df: DataFrame): DataFrame = {
  val replaceTelegestionadoUDF = udf((code: String) => telegestionadoBroadcast.value.getOrElse(code, code))
  df.withColumn("Telegestionado", replaceTelegestionadoUDF(col("Telegestionado")))
}

// Applying the replacement to each DF
val finalUpdatedDataFrames = fullyUpdatedDataFrames.map(replaceTelegestionado)

finalUpdatedDataFrames.foreach { df =>
  display(df)
}

// DBTITLE 1,Replace Disponibilidad ICP
// Function to replace Disponibilidad ICP codes with names
def replaceDisponibilidadICP(df: DataFrame): DataFrame = {
  val replaceDisponibilidadICPUDF = udf((code: String) => disponibilidadICPBroadcast.value.getOrElse(code, code))
  df.withColumn("Disponibilidad ICP", replaceDisponibilidadICPUDF(col("Disponibilidad ICP")))
}

// Applying the replacement to each DF
val allUpdatedDataFrames = finalUpdatedDataFrames.map(replaceDisponibilidadICP)

allUpdatedDataFrames.foreach { df =>
  display(df)
}
