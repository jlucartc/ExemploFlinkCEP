package FlinkCEPClasses

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.flink.api.common.functions.MapFunction

case class S2PlacaMapFunction() extends MapFunction[String,(String,Timestamp,Double,Double)] {
    
    override def map(value: String): (String,Timestamp, Double,Double) = {
    
    
            val tuple = value.replaceAllLiterally("(","").replaceAllLiterally(")","").split(',')
    
            val formatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val date: Date = formatter.parse(tuple(1))
            val timeStampDate: Timestamp = new Timestamp(date.getTime());
 
            (tuple(0),timeStampDate,tuple(2).toDouble,tuple(3).toDouble)
        
    }
}
