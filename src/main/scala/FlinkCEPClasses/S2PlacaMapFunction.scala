package FlinkCEPClasses

import org.apache.flink.api.common.functions.MapFunction

case class S2PlacaMapFunction() extends MapFunction[String,(String,String,String)] {
    
    override def map(value: String): (String, String, String) = {
    
    
            var tuple = value.replaceAllLiterally("(","").replaceAllLiterally(")","").split(',')
        
            (tuple(1),tuple(2),tuple(3))
        
    }
}
