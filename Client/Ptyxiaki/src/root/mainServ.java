package root;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
		String id;
		String filename = request.getParameter("filename");
		String address = request.getParameter("server_address");
		String port = request.getParameter("server_port");
		String delimeter = request.getParameter("delimeter");		
		String has_header = request.getParameter("has_header");
		String unique_keys = request.getParameter("unique_keys");
		String time = request.getParameter("time");
		String first_time = request.getParameter("first_time");

		try {
			id = insertDB(request);	// Add task to DB
			System.out.println("Task ID from db" + id);
			
			createTask(id, filename, address, port, delimeter,has_header, unique_keys,first_time, time); }	// Create a task
		
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace(); }
		

		response.sendRedirect("index.jsp");
	}
	

	
	public static void createTask(String id, String filename, String add,String port, String delim, String has_header, String keys,String first_time, String time) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		Integer seconds = Integer.parseInt(time);
		
		// Create task
		Task task = new Task(id, filename, add,port, delim, has_header, keys,first_time, time);		
		
		// Schedule task
		ScheduledFuture<?> sf = scheduler.scheduleAtFixedRate(task, seconds, seconds , TimeUnit.SECONDS);

		tasks.put(id, sf);
	}
	public static String insertDB(HttpServletRequest r) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		Connection connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
		Statement stmt = (Statement) connection.createStatement();

		String sql = "INSERT INTO tasks (has_header,filename,server_address,server_port,delimeter,unique_keys,first_time,time,active) "+
		" VALUES ('"+r.getParameter("has_header")+"','" + r.getParameter("filename") +
		"','" + r.getParameter("server_address") + "','" + r.getParameter("server_port") + "','" + r.getParameter("delimeter") +
		"','" + r.getParameter("unique_keys") + "','1','" +r.getParameter("time") +"',1)";
		
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
	String taskID;
    String filename;
    String address;
    String server_port;
    String delimeter;
    String has_header;
    String unique_keys;
    String time;
    String first_time;

    public Task(String taskID, String name, String address, String server_port, String delimeter, String has_header, String unique_keys, String first_time, String time) {
        this.taskID = taskID;
        this.filename = name;
        this.address = address;
        this.server_port = server_port;
        this.delimeter = delimeter;
        this.has_header = has_header;
        this.unique_keys = unique_keys;
        this.time = time;
        this.first_time = first_time;
    }
    public Task(){
    }
 
    @Override
    public void run() 
    {
        try {
	    	MyThread th = new MyThread(this);
			th.start();
			th.join();
			if(this.first_time.equals("1")) {
				
				this.first_time = "0";
				String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
				Class.forName("com.mysql.jdbc.Driver").newInstance(); 
				Connection connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
				Statement stmt = (Statement) connection.createStatement();

				String sql = "UPDATE tasks SET first_time ='0' WHERE id = "+ this.taskID;				
				stmt.executeUpdate(sql);
				
			}
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
