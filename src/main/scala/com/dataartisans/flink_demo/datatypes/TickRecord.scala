package com.dataartisans.flink_demo.datatypes

import java.util.Locale
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

class TickRecord (
                   var Id: String,
                   var time: DateTime,
                   var a: String,
                   var b: String,
                   var c: String,
                   var d: String,
                   var e: String
                 ) {

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(Id).append(",")
    sb.append(time.toString(TickRecord.TimeFormatter2)).append(",")
    sb.append(a).append(",")
    sb.append(b).append(",")
    sb.append(c).append(",")
    sb.append(d).append(",")
    sb.append(e)
    sb.toString()
  }
}
object TickRecord {

  @transient
  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyyMMddHHmmssSS").withLocale(Locale.CHINA).withZoneUTC
  final val TimeFormatter2: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.CHINA).withZoneUTC

  def fromString(line: String): TickRecord = {

    val tokens: Array[String] = line.split(",")
    if (tokens.length != 7) {
      throw new RuntimeException("Invalid record: " + line)
    }

      val Id = tokens(0)
      val time = DateTime.parse(tokens(1), TimeFormatter)
      val a = tokens(2)
      val b = tokens(3)
      val c = tokens(4)
      val d = tokens(5)
      val e = tokens(6)

      new TickRecord(Id, time, a, b, c, d, e)

  }

}
