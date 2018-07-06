package com.manojmallela.spark

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.JavaConversions._


object ReadAvro {

  var _schema: Schema = _
  var _sqlSchema: StructType = _

  class TaskReaderFactory(file: File, schemaString: String, sqlSchema: StructType) extends DataReaderFactory[Row] with Serializable {

    _schema = Schema.parse(schemaString)
    _sqlSchema = sqlSchema
    override def createDataReader(): DataReader[Row] = new TaskReader(file)

  }


  class TaskReader(file: File) extends DataReader[Row] with Serializable {

    val reader: FileReader[GenericRecord] = DataFileReader.openReader(file, new GenericDatumReader[GenericRecord]())

    override def next(): Boolean = {
      reader.hasNext
    }

    override def get(): Row = {

      val avroRecord = reader.next()
      val avroSize = avroRecord.getSchema.getFields.size()
      var buffer = new mutable.ListBuffer[Any]
      for (i <- 0 until avroSize){
        val element = avroRecord.get(i)
        if (element != null) buffer.add(element.toString)
        else buffer.add("")
      }
      Row.fromSeq(buffer)
    }

    override def close(): Unit = {
      reader.close()
    }
  }

}
