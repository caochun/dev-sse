package com.dataartisans.flink_demo.sinks

import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext

class HiveSink(outputpath:String) extends RichSinkFunction[String]{
  val filethreshold = 200000

  var output:TextOutputFormat[String] = null
  var linecount = 0
  var filecount = 1
  var context: StreamingRuntimeContext = null

  override def invoke(r: String): Unit ={
    this.output.writeRecord(r)
    this.linecount += 1
    if(this.linecount >= filethreshold){
      this.linecount = 0
      this.output.close()
      this.filecount += 1
      this.output.open(this.filecount*100 + this.context.getIndexOfThisSubtask, this.context.getNumberOfParallelSubtasks)
    }
  }

  @throws[Exception]
  override def open(parameters: Configuration): Unit ={
    this.context = getRuntimeContext.asInstanceOf[StreamingRuntimeContext]

    val format = new TextOutputFormat[String](new Path(outputpath))
    format.setWriteMode(FileSystem.WriteMode.NO_OVERWRITE)
    format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS)
    format.open(this.filecount*100 + this.context.getIndexOfThisSubtask, this.context.getNumberOfParallelSubtasks)
    this.output=format
  }

  override def close(): Unit = {
    if(this.output!=null){
      this.output.close()
    }
  }

}
