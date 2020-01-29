import java.sql.{Array => _, _}
import org.postgresql.Driver


// JDBC driver name and database URL
val JDBC_DRIVER = "org.postgresql.Driver";
val DB_URL = "jdbc:postgresql://localhost:5432/mydb";

//  Database credentials
val USER = "luca";
val PASS = "root";


var conn : Connection = null;
var stmt : Statement = null;
try{
    //STEP 2: Register JDBC driver
    Class.forName("org.postgresql.Driver");
    
    //STEP 3: Open a connection
    println("Connecting to a selected database...");
    conn = DriverManager.getConnection(DB_URL, USER, PASS);
    println("Connected database successfully...");
    
    //STEP 4: Execute a query
    println("Inserting records into the table...");
    stmt = conn.createStatement();
    
    val sql : String = "INSERT INTO placas " +
    "VALUES ('111', '2019-08-12 10:00:00', '(1,2)')";
    stmt.executeUpdate(sql);
    println("Inserted records into the table...");
    
}catch{

    case e : SQLException => e.printStackTrace()
    case e : Exception => e.printStackTrace()

}finally{

    //finally block used to close resources
    try{
        if(stmt!=null)
            conn.close();
    }catch{
        case e : Exception => ()
    }
    
    try{
        if(conn!=null) {
            conn.close()
        }
    }catch{
        case e : SQLException => e.printStackTrace();
    }

}

println("Goodbye!");

