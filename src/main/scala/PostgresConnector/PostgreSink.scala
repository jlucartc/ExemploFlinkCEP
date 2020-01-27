package PostgresConnector

import java.sql.{PreparedStatement,DriverManager}
import java.util.{Properties,UUID}
import org.postgresql.Driver
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory
import org.slf4j.Logger



class PostgreSink(props : Properties, config : ExecutionConfig) extends TwoPhaseCommitSinkFunction[(String,String,String),String,String](createTypeInformation[String].createSerializer(config),createTypeInformation[String].createSerializer(config)){
    
    private var transactionMap : Map[String,(String,String,String)] = Map()
    
    private var parsedQuery : PreparedStatement = _
    
    private val insertionString : String = "INSERT INTO mydb.placas (placa,timestamp,ponto) values (?,?,?)"
    
    private var isset : Boolean = false
    
    override def invoke(transaction: String, value: (String,String,String), context: SinkFunction.Context[_]): Unit = {
        
        val item = this.transactionMap.get(transaction)
        this.transactionMap += (transaction -> (value._1,value._2,value._3))
        
    }
    
    override def beginTransaction(): String = {
        
        if(!this.isset) {
    
            val LOG: Logger = LoggerFactory.getLogger(classOf[PostgreSink])
            LOG.info("Configurando parametros da conexão com BD")
            val driver = props.getProperty("driver")
            val url = props.getProperty("url")
            val user = props.getProperty("user")
            val password = props.getProperty("password")
            Class.forName(driver)
            val connection = DriverManager.getConnection(url + "?user=" + user + "&password=" + password)
            this.parsedQuery = connection.prepareStatement(insertionString)
        
            this.isset = true
            
        }
        
        val identificador = UUID.randomUUID.toString
        
        val m : Option[(String,String,String)]= this.transactionMap.get(identificador)
        
        var hasId  = true
        
        while(hasId){
            
            if( m == None ) {
                this.transactionMap += (identificador -> ("", "",""))
            }
    
    
        }
        
        identificador
        
    }
    
    override def preCommit(transaction: String): Unit = {}
    
    override def commit(transaction: String): Unit = {
    
        try{
            
            this.transactionMap.foreach{
                
                (t) => {
                    
                    this.parsedQuery.setString(1,t._2._1)
                    this.parsedQuery.setString(2,t._2._2)
                    this.parsedQuery.setString(3,t._2._3)
    
                    val LOG : Logger = LoggerFactory.getLogger(classOf[PostgreSink])
                    
                    LOG.info("Adicionando transação ao batch...")
                    
                }
                
            }
            
        }catch{
    
            case e : Exception => {
                val LOG : Logger = LoggerFactory.getLogger(classOf[PostgreSink])
                LOG.info("Erro ao adicionar transação ao batch")
            }
            
        }
        
        try{
            
            this.parsedQuery.executeBatch
            val LOG : Logger = LoggerFactory.getLogger(classOf[PostgreSink])
            LOG.info("Executando batch...")
            
        }catch{
    
            case e : Exception => {
                val LOG : Logger = LoggerFactory.getLogger(classOf[PostgreSink])
                LOG.info("Erro ao executar batch")
            }
            
        }
    
    }
    
    override def abort(transaction: String): Unit = {
        
        this.transactionMap = Map()
        
    }
    
    override def open(param: Configuration): Unit = {

    }
    
}
