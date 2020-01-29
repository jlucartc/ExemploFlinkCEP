import java.sql.{DriverManager, PreparedStatement, SQLException}
import java.util.{Properties, UUID}
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.postgresql.Driver

var properties : Properties = new Properties()

properties.setProperty("driver","org.postgresql.Driver")
properties.setProperty("url","jdbc:postgresql://localhost:5432/mydb")
properties.setProperty("user","luca")
properties.setProperty("password","root")

val query : String = "INSERT INTO placas (placa,timestamp,ponto) values (?,?,point(?,?))"

val driver = properties.getProperty("driver")
Class.forName(driver)
val connection = DriverManager.getConnection(properties.getProperty("url") + "?user=" + properties.getProperty("user") + "&password=" + properties.getProperty("password"))
var parsedQuery = connection.prepareStatement(query)

var t = ("t",("1111111111","2020-01-28 10:00:00","1","1"))

try {
    
    val formatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = formatter.parse(t._2._2);
    val timeStampDate: Timestamp = new Timestamp(date.getTime());
    
    println(timeStampDate.toString)
    
    parsedQuery.setString(1,t._2._1)
    parsedQuery.setString(2,timeStampDate.toString)
    parsedQuery.setString(3,t._2._3)
    parsedQuery.setString(4,t._2._4)
    parsedQuery.addBatch()
    
    println(parsedQuery.toString)
    
}catch{
    
    case e : Exception => { println("Erro") }
    
}