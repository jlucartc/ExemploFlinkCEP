package PostgresConnector

import java.util._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._

class PostgreSink(props : Properties, config : ExecutionConfig) extends TwoPhaseCommitSinkFunction[String,String,String](createTypeInformation[String].createSerializer(config),createTypeInformation[String].createSerializer(config)){
    
    private var transactionArray : Array[Array[String]] = Array()
    
    override def invoke(transaction: String, value: String, context: SinkFunction.Context[_]): Unit = {}
    
    override def beginTransaction(): String = {
        
        this.transactionArray = this.transactionArray ++ Array(Array("timestamp"))

        "timestamp"
        
    }
    
    override def preCommit(transaction: String): Unit = {}
    
    override def commit(transaction: String): Unit = {
    
        try{
            
            this.transactionArray.foreach{
                
                (t) => {
                    
                    /* realiza a transação */
                    
                }
                
            }
            
        }catch{
    
            case e : Exception =>
                    println("Erro")
            
            
        }
    
    }
    
    override def abort(transaction: String): Unit = {}
}
