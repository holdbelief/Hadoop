import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args)  throws SQLException{
		try {
			Class.forName(driverName);
		} catch ( ClassNotFoundException e ) {
			e.printStackTrace();
			System.exit(1);
		}
		
		Connection con = DriverManager.getConnection("jdbc:hive2://faith-Fedora:10000/default", "faith", "");
		Statement stmt = con.createStatement();
		String sql = "create database hive";
		System.out.println("Running: " + sql);
		
		int i = stmt.executeUpdate(sql);
		
		sql = "use hive";
		System.out.println("Running: " + sql);
		stmt.execute(sql);
		
		String tableName = "testHiveDriverTable";
		stmt.execute("drop table " + tableName);
		int y = stmt.executeUpdate("create table " + tableName + " (key int, value string)");
		
		sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet rs = stmt.executeQuery(sql);
		if ( rs.next() ) {
			System.out.println(rs.getString(1));
		}
		
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		
		rs = stmt.executeQuery(sql);
		while ( rs.next() ) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}
		
		
	}
}




































