import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink

var tableSink = JDBCAppendTableSink.builder()
.setBatchSize(1)
.setDBUrl("jdbc:postgres://localhost:5432/mydb")
.setDrivername("org.postgres.Driver")
.setPassword("root")
.setUsername("luca")
.setQuery("INSERT INTO placas (placa,timestamp,ponto) VALUES (?,?,point(?,?))")
.setParameterTypes(TypeInformation.of(classOf[String]),TypeInformation.of(classOf[String]),TypeInformation.of(classOf[String]),TypeInformation.of(classOf[String]))
.build()

println(tableSink.getClass.toString)