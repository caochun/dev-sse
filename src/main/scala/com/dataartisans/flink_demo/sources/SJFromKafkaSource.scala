package com.dataartisans.flink_demo.sources

import com.dataartisans.flink_demo.datatypes.{TickRecord}
import com.dataartisans.flink_demo.utils.KafkaConsumerTool2
import kafka.consumer.KafkaStream
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

class SJFromKafkaSource (group:String) extends SourceFunction[TickRecord] {
  private var consumer: KafkaConsumerTool2 = null
  override def cancel(): Unit = {}


  override def run(sourceContext: SourceFunction.SourceContext[TickRecord]): Unit = {
    if(this.consumer == null){
      this.consumer = new KafkaConsumerTool2("host10:2181", "sj", group)
    }

    while(true) {
      val records: KafkaStream[Array[Byte], Array[Byte]] = this.consumer.getRecords

      val it = records.iterator()
      while (it.hasNext) {
        val ne = it.next()
        var  message = new String(ne.message())


        System.out.print(message+"\n")
        if (message.length > 30) {
          val waste = TickRecord.fromString(message)
          sourceContext.collectWithTimestamp(waste, waste.time.getMillis)
          sourceContext.emitWatermark(new Watermark(waste.time.getMillis - 1000))
        }
      }
    }

  }
}
