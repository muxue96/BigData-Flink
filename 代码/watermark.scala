import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

//创建样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object watermark {
  def main(args: Array[String]): Unit = {
    //引用隐式转换，不然后面的map和flatMap会报错
    import org.apache.flink.streaming.api.scala._
    //创建执行环境
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置事件时间语义
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从scocket中读取数据
    val inputStream = senv.socketTextStream("master", 7777)
    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
          override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L//转成毫秒数
        })//设置watermark
    //定义一个OutputTag
    val latetag = new OutputTag[(String, Double, Long)]("late")
    //对数据流进行开窗和转换操作
    val stream = dataStream
      .map(data => (data.id,data.temperature,data.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(latetag)
      .reduce((oldData,newData) => (oldData._1,oldData._2.min(newData._2),newData._3))
    //获取并打印侧输出流
    stream.getSideOutput(latetag).print("late")
    //打印输出主流
    stream.print("result")
    //触发程序执行
    senv.execute("window test")
  }
}


/*
sensor_1,1547718199,35.8
sensor_6,1547718201,15.4
sensor_7,1547718202,6.7
sensor_7,1547718203,8.4
sensor_1,1547718205,38.1
sensor_1,1547718206,32.0
sensor_1,1547718208,11.1

sensor_1,1547718210,39.2
sensor_1,1547718213,30.9
sensor_1,1547718212,28.1

sensor_1,1547718225,29
sensor_1,1547718228,32.3
sensor_1,1547718215,40.1

sensor_1,1547718285,28
sensor_1,1547718218,24.4

sensor_1,1547718288,30
sensor_1,1547718219,23*/