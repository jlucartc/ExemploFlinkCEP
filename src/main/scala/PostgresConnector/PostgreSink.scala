package PostgresConnector

import java.sql.{BatchUpdateException, DriverManager, PreparedStatement, SQLException, Timestamp}
import java.text.ParseException
import java.util.{Date, Properties, UUID}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._
import org.slf4j.{Logger, LoggerFactory}




class PostgreSink(props : Properties, config : ExecutionConfig) extends TwoPhaseCommitSinkFunction[(String,Timestamp,Double,Double),String,String](createTypeInformation[String].createSerializer(config),createTypeInformation[String].createSerializer(config)){
    
    private var transactionMap : Map[String,Array[(String,Timestamp,Double,Double)]] = Map()
    
    private var parsedQuery : PreparedStatement = _
    
    private val insertionString : String = "INSERT INTO placas (placa,timestamp,ponto) values (?,?,point(?,?))"
    
    override def invoke(transaction: String, value: (String,Timestamp,Double,Double), context: SinkFunction.Context[_]): Unit = {
    
        val LOG = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
        
        val res = this.transactionMap.get(transaction)
        
        if(res.isDefined){
    
            var array = res.get
            
            array = array ++ Array(value)
    
            this.transactionMap += (transaction -> array)
            
        }else{
    
            val array = Array(value)
    
            this.transactionMap += (transaction -> array)
            
            
        }
    
        LOG.info("\n\nPassing through invoke\n\n")
        
        ()
        
    }
    
    override def beginTransaction(): String = {
    
        val LOG: Logger = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
        
        val identifier = UUID.randomUUID.toString
    
        LOG.info("\n\nPassing through beginTransaction\n\n")
        
        identifier
        
        
    }
    
    override def preCommit(transaction: String): Unit = {
        
        val LOG = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
    
        try{
        
            val tuple : Option[Array[(String,Timestamp,Double,Double)]]= this.transactionMap.get(transaction)
        
            if(tuple.isDefined){
            
                tuple.get.foreach( (value : (String,Timestamp,Double,Double)) => {
                
                    LOG.info("\n\n"+value.toString()+"\n\n")
                
                    this.parsedQuery.setString(1,value._1)
                    this.parsedQuery.setString(2,value._2.toString)
                    this.parsedQuery.setString(3,value._3.toString)
                    this.parsedQuery.setString(4,value._4.toString)
                    this.parsedQuery.addBatch()
                
                })
                
            }
        
        }catch{
        
            case e : SQLException =>
                LOG.info("\n\nError when adding transaction to batch: SQLException\n\n")
        
            case f : ParseException =>
                LOG.info("\n\nError when adding transaction to batch: ParseException\n\n")
        
            case g : NoSuchElementException =>
                LOG.info("\n\nError when adding transaction to batch: NoSuchElementException\n\n")
        
            case h : Exception =>
                LOG.info("\n\nError when adding transaction to batch: Exception\n\n")
        
        }
        
        this.transactionMap = this.transactionMap.empty
    
        LOG.info("\n\nPassing through preCommit...\n\n")
    }
    
    override def commit(transaction: String): Unit = {
    
        val LOG : Logger = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
        
        if(this.parsedQuery != null) {
            LOG.info("\n\n" + this.parsedQuery.toString+ "\n\n")
        }
        
        try{
            
            this.parsedQuery.executeBatch
            val LOG : Logger = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
            LOG.info("\n\nExecuting batch\n\n")
            
        }catch{
    
            case e : SQLException =>
                val LOG : Logger = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
                LOG.info("\n\n"+"Error : SQLException"+"\n\n")
            
        }
        
        this.transactionMap = this.transactionMap.empty
    
        LOG.info("\n\nPassing through commit...\n\n")
        
    }
    
    override def abort(transaction: String): Unit = {
    
        val LOG : Logger = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
        
        this.transactionMap = this.transactionMap.empty
    
        LOG.info("\n\nPassing through abort...\n\n")
        
    }
    
    override def open(parameters: Configuration): Unit = {
    
        val LOG: Logger = LoggerFactory.getLogger(classOf[FlinkCEPClasses.FlinkCEPPipeline])
        
        val driver = props.getProperty("driver")
        val url = props.getProperty("url")
        val user = props.getProperty("user")
        val password = props.getProperty("password")
        Class.forName(driver)
        val connection = DriverManager.getConnection(url + "?user=" + user + "&password=" + password)
        this.parsedQuery = connection.prepareStatement(insertionString)
    
        LOG.info("\n\nConfiguring BD conection parameters\n\n")
    }
}
