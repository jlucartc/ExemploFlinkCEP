package FlinkCEPClasses

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.sinks.TableSink
import org.postgresql.Driver

class TableAPIPipeline {
    
    // --- From FlinkCEPPipeline ---
    
    var env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.enableCheckpointing(10)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    
    //var input : DataStream[String] = env.readFile(new TextInputFormat(new Path("/home/luca/Desktop/lines")),"/home/luca/Desktop/lines",FileProcessingMode.PROCESS_CONTINUOUSLY,1)
    
    var input : DataStream[String] = env.readTextFile("/home/luca/Desktop/lines").name("Stream original")
    
    var tupleStream : DataStream[(String,Timestamp,Double,Double)] = input.map(new S2PlacaMapFunction()).name("Tuple Stream")
    
    var properties : Properties = new Properties()
    
    properties.setProperty("driver","org.postgresql.Driver")
    properties.setProperty("url","jdbc:postgresql://localhost:5432/mydb")
    properties.setProperty("user","luca")
    properties.setProperty("password","root")
    
    // --- From FlinkCEPPipeline END ---
    
    
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,settings)
    
    var tableSink : JDBCAppendTableSink = JDBCAppendTableSink.builder()
    .setBatchSize(1)
    .setDBUrl("jdbc:postgresql://localhost:5432/mydb")
    .setDrivername("org.postgresql.Driver")
    .setPassword("root")
    .setUsername("luca")
    .setQuery("INSERT INTO placas (placa,timestamp,ponto) VALUES (?,?,point(?,?))")
    .setParameterTypes(Types.STRING,Types.SQL_TIMESTAMP,Types.DOUBLE,Types.DOUBLE)
    .build()
    
    val fieldNames = Array("placa","timestamp","latitude","longitude")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING,Types.SQL_TIMESTAMP,Types.DOUBLE, Types.DOUBLE)
    
    /*tableEnv.registerDataStream(
        "placas",
                tupleStream
    )*/
    
    
    tableEnv.registerTableSink("postgres-table-sink",
        fieldNames,
        fieldTypes,
        tableSink
    )
    
    var table = tableEnv.fromDataStream(tupleStream)
    
    table.insertInto("postgres-table-sink")
    
    
    
    
    env.execute()
    
}
