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

    /**
     * Default constructor. 
     */
	
    public MyServletContextListener() {
        // TODO Auto-generated constructor stub

        root.mainServ.tasks = new HashMap<>();
		root.mainServ.scheduler = Executors.newScheduledThreadPool(4);
    	System.out.println("CONSTRUCTOR!~~~~~~~~~~~~~~~");
    }

	/**
     * @see ServletContextListener#contextDestroyed(ServletContextEvent)
     */
    public void contextDestroyed(ServletContextEvent arg0)  { 
         // TODO Auto-generated method stub
    }

	/**
     * @see ServletContextListener#contextInitialized(ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent arg0) { 
         // TODO Auto-generated method stub
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
				  String source = rs.getString("source");
				  Integer time = rs.getInt("time");

				  mainServ.myScheduler(id,source, time);
			}

			System.out.println("Updated Scheduler");
			
			connection.close();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	
    	
    }
	
}
