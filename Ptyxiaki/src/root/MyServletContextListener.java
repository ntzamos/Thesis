package root;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Executors;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

/**
 * Application Lifecycle Listener implementation class MyServletContextListener
 *
 */
@WebListener
public class MyServletContextListener implements ServletContextListener {

	
    public MyServletContextListener() {
        root.mainServ.tasks = new HashMap<>();
		root.mainServ.scheduler = Executors.newScheduledThreadPool(4);
    	System.out.println("CONSTRUCTOR!~~~~~~~~~~~~~~~");
    }

	/**
     * @see ServletContextListener#contextDestroyed(ServletContextEvent)
     */
    public void contextDestroyed(ServletContextEvent arg0)  { 
         
    }

	/**
     * @see ServletContextListener#contextInitialized(ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent arg0) { 
        
    	System.out.println("STARTEDDDDD FROM THE BOTTOM!!!!!!!!!!!!!!!!!");
    
		try {
			
	    	String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
			Connection connection = null; 
			Class.forName("com.mysql.jdbc.Driver").newInstance(); 
			connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
			
			Statement stmt = (Statement) connection.createStatement();
			
			 
			String sql = "SELECT * FROM tasks WHERE active = 1;";
			ResultSet rs;
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String id = rs.getString("id");
				String filename = rs.getString("filename");
				String address = rs.getString("server_address");
				String delimeter = rs.getString("delimeter");
				String unique_keys = rs.getString("unique_keys");
				String time = rs.getString("time");

				mainServ.createTask(id, filename, address, delimeter, unique_keys, time);
			}

			System.out.println("Updated Scheduler");
			
			connection.close();
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

    	
    	
    }
	
}
