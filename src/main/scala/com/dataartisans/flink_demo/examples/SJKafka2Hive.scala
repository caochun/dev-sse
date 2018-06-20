/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.examples

import java.text.SimpleDateFormat

import com.dataartisans.flink_demo.datatypes.{GeoPoint, TickRecord, WasteRecord}
import com.dataartisans.flink_demo.sinks.{ElasticsearchUpsertSink, HiveSink}
import com.dataartisans.flink_demo.sources.{SJFromKafkaSource, WasteRecordSource, WateRecordFromKafkaSource}
import com.dataartisans.flink_demo.utils.DemoStreamEnvironment
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer
import org.apache.hadoop.io.IntWritable
import org.joda.time.DateTime

object SJKafka2Hive {

  def main(args: Array[String]) {
    System.setProperty("es.set.netty.runtime.available.processors", "false")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val rides: DataStreamSource[TickRecord] = env.addSource(new SJFromKafkaSource(args(0)))

    val cntByLocation = rides
      //      .map{ (_.wasteId, _.time.getMillis, _.location, _. stationId, _.vlp, _.stationName)}
      .map(new StringMapper)

    cntByLocation.addSink(new HiveSink(args(1)))

    env.execute("sj from kafka to hdfs ")

  }

  class StringMapper extends MapFunction[TickRecord, String] {

    def map(x: TickRecord): String = {
      return x.toString()
    }
  }

}

