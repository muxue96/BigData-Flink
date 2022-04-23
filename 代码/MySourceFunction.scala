import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = senv.addSource(new MySensorReading())
    stream.print()
    senv.execute("MySourceFunction")
  }
}
//自定义SourceFunction
class MySensorReading() extends SourceFunction[SensorReading]{
  var running: Boolean = true
  override def cancel() : Unit = running = false
  //定义一个随机数发生器
  val rand = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    //随机生成一组（10个）传感器的初始温度：(id,temp)
    var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))
    //定义无线循环，不停地产生数据，除非被cancel
    while (running) {
      //在上次数据基础上微调，更新温度值
      curTemp = curTemp.map(
        data => (data._1,data._2+rand.nextGaussian())
      )
      //获取当前时间戳，加入到数据中,调用sourceContext的collect方法把数据发送出去
      val curTime = System.currentTimeMillis()
      curTemp.foreach(data => sourceContext.collect(SensorReading(data._1,curTime,data._2)))
      //间隔100ms
      Thread.sleep(10000)
    }
  }
}