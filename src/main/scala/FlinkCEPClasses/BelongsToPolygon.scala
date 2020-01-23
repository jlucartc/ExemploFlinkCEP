package FlinkCEPClasses

import org.apache.flink.cep.pattern.conditions.IterativeCondition

class BelongsToPolygon() extends IterativeCondition[String]{
    override def filter(value: String, ctx: IterativeCondition.Context[String]): Boolean = {
    
         true
    
    }
}
