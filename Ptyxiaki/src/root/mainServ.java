package root;


import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
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

/**
 * Servlet implementation class mainServ
 */
@WebServlet("/mainServ")
public class mainServ extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	public static Map<String, ScheduledFuture<?>> tasks;
	public static ScheduledExecutorService scheduler;
	
    public mainServ() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub

//		Process proc = Runtime.getRuntime().exec("/home/cluster/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class com.mycompany.app.App --master mesos://node7:5050 /home/cluster/ptyxiaki/Testapp/test-app/target/test-app-1.0-SNAPSHOT.jar");
//		
//		BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
//		String s = null;
//		String ans = "";
//		while ((s = stdInput.readLine()) != null) {
//			ans += s + "\n";
//		}
		response.getWriter().append("Served at: ").append(request.getParameter("id"));
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	
	public static void myScheduler(String id, String name, Integer seconds) {
		 ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(new Runnable() {
		    public void run() { 
		    	//MyThread th = new MyThread("/home/cluster/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class com.mycompany.app.App --master mesos://node7:5050 /home/cluster/ptyxiaki/Testapp/test-app/target/test-app-1.0-SNAPSHOT.jar");
				String filename = name + Integer.toString((int)(Math.random()*1000));
		    	MyThread th = new MyThread("cp init " + filename ,  filename);
				th.start();
		    }
		}, seconds,seconds, TimeUnit.SECONDS);		

		tasks.put(id, task);
	}
	public String insertDB(String name, String time) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
		Connection connection = null; 
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
		
		Statement stmt = (Statement) connection.createStatement();
		
		 
		String sql = "INSERT INTO tasks (source,destination,time,active) VALUES ('"+ name +"','','"+time+"',1)";
		stmt.executeUpdate(sql);
		
		ResultSet rs = stmt.executeQuery("select MAX(id) as last_id from tasks");
		rs.next();
		Integer lastid = rs.getInt("last_id");

		System.out.println("hi");
		
		connection.close();
		return lastid.toString();
		
	}
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		

		String name = request.getParameter("name");
		Integer seconds = Integer.parseInt(request.getParameter("seconds"));
		
		
		
		try {
			String id = insertDB(name, seconds.toString());
			myScheduler(id,name, seconds);
			
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		response.sendRedirect("index.jsp");
//	    
	}

}
