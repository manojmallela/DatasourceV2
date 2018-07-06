package com.manojmallela.spark

import java.io.File
import java.util

import com.databricks.spark.avro.SchemaConverters
import com.manojmallela.spark.ReadAvro.TaskReaderFactory
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

class AvroDatasourceV2 extends DataSourceV2 with ReadSupport {


  class Reader(dataSourceOptions: DataSourceOptions) extends DataSourceReader with Serializable {


    private val schemas = getSchema(dataSourceOptions.get("path").get())

    // Infer schema from sample
    override def readSchema(): StructType = {
      val localSchema = StructType(StructField("FEATURE_ID", StringType, nullable = true) ::
        StructField("FEATURE_NAME", StringType, nullable = true) ::
        StructField("FEATURE_CLASS", StringType, nullable = true) ::
        StructField("STATE_ALPHA", StringType, nullable = true) ::
        StructField("STATE_NUMERIC", StringType, nullable = true) ::
        StructField("COUNTY_NAME", StringType, nullable = true) ::
        StructField("COUNTY_NUMERIC", StringType, nullable = true) ::
        StructField("PRIMARY_LAT_DMS", StringType, nullable = true) ::
        StructField("PRIM_LONG_DMS", StringType, nullable = true) ::
        StructField("PRIM_LAT_DEC", StringType, nullable = true) ::
        StructField("PRIM_LONG_DEC", StringType, nullable = true) ::
        StructField("SOURCE_LAT_DMS", StringType, nullable = true) ::
        StructField("SOURCE_LONG_DMS", StringType, nullable = true) ::
        StructField("SOURCE_LAT_DEC", StringType, nullable = true) ::
        StructField("SOURCE_LONG_DEC", StringType, nullable = true) ::
        StructField("ELEV_IN_M", StringType, nullable = true) ::
        StructField("ELEV_IN_FT", StringType, nullable = true) ::
        StructField("MAP_NAME", StringType, nullable = true) ::
        StructField("DATE_CREATED", StringType, nullable = true) ::
        StructField("DATE_EDITED", StringType, nullable = true) :: Nil)
      localSchema
    }


    override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

      val readerFactories = new util.ArrayList[DataReaderFactory[Row]]()

      val partitions = new util.ArrayList[File]()

      val inputPath = new File(dataSourceOptions.get("path").get())
      if (inputPath.isFile && inputPath.getName.endsWith(".avro")) {
        partitions.add(inputPath)
      } else if (inputPath.isDirectory) {
        inputPath
          .listFiles()
          .filter(_.getName.endsWith(".avro"))
          .foreach(partitions.add)
      }

      for (partition <- partitions) {
        readerFactories.add(new TaskReaderFactory(partition, schemas._1.toString, schemas._2))
      }

      readerFactories
    }


  }


  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader(options)


  def getSchema(path: String): (Schema, StructType) = {
    val inputPath = new File(path)
    var sample: File = null

    if (inputPath.isFile && inputPath.getName.endsWith(".avro")) {
      sample = inputPath
    } else if (inputPath.isDirectory) {
      sample = inputPath.listFiles().filter(_.getName.endsWith(".avro")).head
    }

    val avroSchema = DataFileReader
      .openReader(sample, new GenericDatumReader[GenericRecord]())
      .getSchema

    val sqlSchema = SchemaConverters.toSqlType(avroSchema).dataType match {
      case t: StructType => Some(t).getOrElse {
        throw new Exception("Unable to infer schema")
      }
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:
           |
           |${avroSchema.toString(true)}
           |""".stripMargin)
    }

    (avroSchema, sqlSchema)
  }


}
