package root;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

@WebServlet("/mainServ")
public class mainServ extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	public static Map<String, ScheduledFuture<?>> tasks;
	public static ScheduledExecutorService scheduler;
	
    public mainServ() {
        super();
    }

	
    /** @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)*/
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		response.getWriter().append("GOT GET");
		response.sendRedirect("index.jsp");
	}

	
	
	/** @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)*/
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		try {
			createTask(request); }	// Create a task
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace(); }
		

		response.sendRedirect("index.jsp");
	}
	

	
	public static void createTask(HttpServletRequest req) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String id;
		String filename = req.getParameter("filename");
		String address = req.getParameter("server_address");
		String delimeter = req.getParameter("delimeter");
		String unique_keys = req.getParameter("unique_keys");
		String time = req.getParameter("seconds");
		Integer seconds = Integer.parseInt(time);
		
		id = insertDB(req);	// Add task to DB
		System.out.println("Task ID from db" + id);
		
		// Create task
		Task task = new Task(id, filename, address, delimeter, unique_keys, time);		
		
		// Schedule task
		ScheduledFuture<?> sf = scheduler.scheduleAtFixedRate(task, seconds, seconds , TimeUnit.SECONDS);

		tasks.put(id, sf);
	}
	public static String insertDB(HttpServletRequest r) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		Connection connection = null; 
		String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		
		connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
		
		Statement stmt = (Statement) connection.createStatement();
		
		 
		String sql = "INSERT INTO tasks (has_header,filename,server_address,delimeter,unique_keys,time,active) "+
		" VALUES (1,'" + r.getParameter("filename") +
		"','" + r.getParameter("server_address") + "','" + r.getParameter("delimeter") +
		"','" + r.getParameter("unique_keys") + "','" +r.getParameter("time") +"',1)";
		stmt.executeUpdate(sql);
		
		ResultSet rs = stmt.executeQuery("select MAX(id) as last_id from tasks");
		rs.next();
		Integer lastid = rs.getInt("last_id");

		
		connection.close();
		return lastid.toString();
		
	}
}

class Task implements Runnable
{
    private String taskID;
    private String filename;
    private String address;
    private String delimeter;
    private String unique_keys;
    private String time;
    
    public Task(String taskID, String name, String address, String delimeter, String unique_keys, String time) {
        this.taskID = taskID;
        this.filename = name;
        this.address = address;
        this.delimeter = delimeter;
        this.unique_keys = unique_keys;
        this.time = time;
        
    }
 
    @Override
    public void run() 
    {
        try {
        	//MyThread th = new MyThread("/home/cluster/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class com.mycompany.app.App --master mesos://node7:5050 /home/cluster/ptyxiaki/Testapp/test-app/target/test-app-1.0-SNAPSHOT.jar");
            System.out.println("Doing a task [" + taskID + "] during : " + filename + " - Time - " + new Date());
            String file = filename + Integer.toString((int)(Math.random()*1000));
	    	MyThread th = new MyThread("cp init " + file ,  file);
			th.start();
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
