package FlinkCEPClasses

import java.sql.Timestamp

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

class TableAPIPipeline {

    var env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    var input : DataStream[String] = env.readTextFile("/home/luca/Desktop/lines")
    .name("Stream original")

    var tupleStream : DataStream[(String,Timestamp,Double,Double)] = input.map(new S2PlacaMapFunction())
    .name("Tuple Stream")

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env,settings)
    var tableSink : JDBCAppendTableSink = JDBCAppendTableSink.builder()
    .setBatchSize(1)
    .setDBUrl("jdbc:postgresql://localhost:5432/flinkdb")
    .setDrivername("org.postgresql.Driver")
    .setPassword("flinkdb")
    .setUsername("flinkdb")
    .setQuery("INSERT INTO placas (placa,timestamp,ponto) VALUES (?,?,point(?,?))")
    .setParameterTypes(Types.STRING,Types.SQL_TIMESTAMP,Types.DOUBLE,Types.DOUBLE)
    .build()
    
    val fieldNames = Array("placa","timestamp","latitude","longitude")
    val fieldTypes = Array[TypeInformation[_]](
        Types.STRING,
        Types.SQL_TIMESTAMP,
        Types.DOUBLE,
        Types.DOUBLE
    )

    tableEnv.registerTableSink("postgres-table-sink",
        fieldNames,
        fieldTypes,
        tableSink
    )
    
    var table = tableEnv.fromDataStream(tupleStream)
    table.insertInto("postgres-table-sink")
    env.execute()
}
