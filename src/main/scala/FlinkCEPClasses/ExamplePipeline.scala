package FlinkCEPClasses

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.Map

class ExamplePipeline {
    
    //var props : Properties = new Properties()
    
    var env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    
    //props.setProperty("bootstrap.servers","localhost:9092")
    //props.setProperty("zookeeper.connect","localhost:2181")
    //props.setProperty("group.id","flink-consumer")
    
    //var input : DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("placas",new SimpleStringSchema(),props))
    
    var input : DataStream[String] = env.readTextFile("/home/luca/Desktop/lines")
    
    var padrao = Pattern.begin[String]("igual").where( (string) => string.length >= 10 )
    val CEPstream: PatternStream[String] = CEP.pattern[String](input,padrao)
    
    val result: DataStream[String] = CEPstream.select(new PatternSelectFunction[String,String](){
        override def select(matches: java.util.Map[String,java.util.List[String]]): String = {
            if(matches.containsKey("igual")) {
                matches.get("igual").get(0)
            }else{
                "ASDASDASDSDAS"
            }
        }
    })
    
    result.writeAsText("/home/luca/Desktop/flinkcepout",FileSystem.WriteMode.OVERWRITE)
    
    env.execute()
    
    
}
