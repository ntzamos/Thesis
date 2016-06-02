package root;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * Application Lifecycle Listener implementation class MyServletContextListener
 *
 */
@WebListener
public class MyServletContextListener implements ServletContextListener {

	
    public MyServletContextListener() {
        root.mainServ.tasks = new HashMap<>();
		root.mainServ.scheduler = Executors.newScheduledThreadPool(4);
    }

	/**
     * @see ServletContextListener#contextDestroyed(ServletContextEvent)
     */
    public void contextDestroyed(ServletContextEvent arg0)  { 
    	for(Map.Entry<String, ScheduledFuture<?>> s: mainServ.tasks.entrySet())
    		s.getValue().cancel(false);
    }

	/**
     * @see ServletContextListener#contextInitialized(ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent arg0) { 
        
    
		try {
			List<String[]> tasks = mainServ.getAllTasks();
			for(String[] tokens: tasks) {
			
				 if(tokens[1].equals("0"))
						mainServ.createCSVTask(tokens[0], tokens[7], tokens[8],tokens[9], tokens[10],tokens[11], tokens[12],tokens[13], tokens[14], tokens[15]);
					else 
						mainServ.createMySQLTask(tokens[0],tokens[2], tokens[3],tokens[4],tokens[5],tokens[6], tokens[8], tokens[9], tokens[10],tokens[11],tokens[14], tokens[15]); 	// Create a task
					
			}

			System.out.println("Updated Scheduler");
			
			
			
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

    	
    	
    }
	
}
