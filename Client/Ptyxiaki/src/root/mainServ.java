package root;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


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
		String ismysql = request.getParameter("mysql");
		
		if(ismysql.equals("0")) {
		
			String filename = request.getParameter("filename");
			String address = request.getParameter("server_address");
			String port = request.getParameter("server_port");
			String delimeter = request.getParameter("delimeter");		
			String has_header = request.getParameter("has_header");
			String unique_keys = request.getParameter("unique_keys");
			String deletes = request.getParameter("deletes");	
			String time = request.getParameter("time");
			String first_time = "1";
	
			try {
				String id = insertDB(request);	// Add task to DB
				System.out.println("Task ID from db" + id);
				
				createCSVTask(id, filename, address, port, delimeter,deletes,has_header, unique_keys,first_time, time); 
				
			}	// Create a task
			
			catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace(); 
				} catch (ClassNotFoundException e) {

				e.printStackTrace();
			}
		} else {

			String database_address = request.getParameter("database_address");
			String database = request.getParameter("database");
			String tablename = request.getParameter("tablename");
			String username = request.getParameter("username");
			String password = request.getParameter("password");

			String address = request.getParameter("server_address");
			String port = request.getParameter("server_port");
			String delimeter = request.getParameter("delimeter");	
			String deletes = request.getParameter("deletes");		
			String time = request.getParameter("time");
			String first_time = "1";

			try {

				String id = insertDB(request);
				System.out.println("Task ID from db" + id);
				createMySQLTask(id, database_address, database,tablename,username,password, address, port, delimeter,deletes,first_time, time); 	// Create a task
			
			} catch (InstantiationException | IllegalAccessException e) {
				
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}

		response.sendRedirect("index.jsp");
	}
	


	public static void createCSVTask(String id, String filename, String add,String port, String delim,String deletes, String has_header, String keys,String first_time, String time) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		Integer seconds = Integer.parseInt(time);
		
		// Create task
		Task task = new Task(id, filename, add,port, delim,deletes, has_header, keys,first_time, time);		
		
		// Schedule task
		ScheduledFuture<?> sf = scheduler.scheduleAtFixedRate(task, seconds, seconds , TimeUnit.SECONDS);

		tasks.put(id, sf);
	}

	public static void createMySQLTask(String id,String dbaddress, String db,String table,String user, String pass, String add,String port, String delim,String deletes, String first_time, String time) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		Integer seconds = Integer.parseInt(time);
		
		// Create task
		Task task = new Task(id,dbaddress, db,table,user,pass, add,port, delim,deletes,first_time, time);		
		
		// Schedule task
		ScheduledFuture<?> sf = scheduler.scheduleAtFixedRate(task, seconds, seconds , TimeUnit.SECONDS);

		tasks.put(id, sf);
	}
	public static List<String[]> getAllTasks() throws IOException {
		
		BufferedReader fileReader = null;
     
    	//Create a new list of student to be filled by CSV file data 
    	List<String[]> tasks = new ArrayList<String[]>();
    	
        String line = "";
        
        //Create the file reader

        File f = new File("mytasks.csv");
        if(!f.exists()) f.createNewFile();
        	
        fileReader = new BufferedReader(new FileReader("mytasks.csv"));
        
        while ((line = fileReader.readLine()) != null) {
        			tasks.add(line.split(";"));
        }
        
        fileReader.close();

        return tasks;
	
	}
	public static List<String[]> getActiveTasks() throws IOException {
		
		BufferedReader fileReader = null;
     
    	//Create a new list of student to be filled by CSV file data 
    	List<String[]> tasks = new ArrayList<String[]>();
    	
        String line = "";
        
        //Create the file reader

        File f = new File("mytasks.csv");
        if(!f.exists()) f.createNewFile();
        	
        fileReader = new BufferedReader(new FileReader("mytasks.csv"));
        
        while ((line = fileReader.readLine()) != null) {
        	if(line.substring(line.length() - 1).equals("1"))
        			tasks.add(line.split(";"));
        }
        
        fileReader.close();

        return tasks;
	
	}
	public static Integer getSizeDB() throws IOException {
		BufferedReader fileReader = null;
	     
    	//Create a new list of student to be filled by CSV file data 
    	List<String[]> tasks = new ArrayList<String[]>();
    	
        String line = "";
        
        //Create the file reader

        File f = new File("mytasks.csv");
        if(!f.exists()) f.createNewFile();
        	
        fileReader = new BufferedReader(new FileReader("mytasks.csv"));
        
        while ((line = fileReader.readLine()) != null) {
   			tasks.add(line.split(";"));
        }
        
        fileReader.close();

        return tasks.size();
		
	}
	public static String insertDB(HttpServletRequest r) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		String task = "";
		String taskid = Integer.toString(getSizeDB()+1);
		if(r.getParameter("mysql").equals("1")) {

			task  = taskid +";1;" + 
			r.getParameter("database_address") +";" + 
			r.getParameter("database") +";"
			+r.getParameter("tablename")+";"
			+r.getParameter("username") +";"
			+r.getParameter("password") +";" 
			+ ";" 
			+r.getParameter("server_address") +";" 
			+r.getParameter("server_port") + ";" 
			+r.getParameter("delimeter") +";"
			+r.getParameter("deletes") +";"
			+ "1;"  
			+ ";"  
			+ "1;" 
			+r.getParameter("time") + ";1";

		}
		else {

			task  = taskid +";0;" 
			+";" 
			+";"
			+";"
			+";"
			+";" 
			+ r.getParameter("filename")+ ";" 
			+ r.getParameter("server_address") +";" 
			+ r.getParameter("server_port") + ";" 
			+ r.getParameter("delimeter") +";"
			+ r.getParameter("deletes") +";"
			+ r.getParameter("has_header") + ";"  
			+ r.getParameter("unique_keys") + ";"  
			+"1;" 
			+r.getParameter("time") + ";1\n";
		}
		System.out.println(task);
		
		FileWriter pw = new FileWriter("mytasks.csv",true); 
		pw.append(task);
		pw.close();
		return taskid;		
	}
}

class Task implements Runnable
{
	String taskID;
	String db;
	String dbaddress;
	String user;
	String table;
	String pass;
    String filename;
    String address;
    String server_port;
    String delimeter;
    String has_header;
    String unique_keys;
    String time;
    String first_time;
    String deletes;
    String mysql;
    Boolean ended;

    public Task(String taskID, String name, String address, String server_port, String delimeter,String deletes, String has_header, String unique_keys, String first_time, String time) {
        this.taskID = taskID;
        this.filename = name;
        this.address = address;
        this.server_port = server_port;
        this.delimeter = delimeter;
        this.has_header = has_header;
        this.unique_keys = unique_keys;
        this.time = time;
        this.first_time = first_time;
        this.mysql = "0";
        this.deletes = deletes;
        this.ended = true;
    }

    public Task(String taskID,String dbaddress, String db, String table, String user, String pass, String address, String server_port, String delimeter,String deletes, String first_time, String time) {
        this.taskID = taskID;
        this.dbaddress = dbaddress;
        this.db = db;
        this.user = user;
        this.table = table;
        this.pass = pass;
        this.address = address;
        this.server_port = server_port;
        this.delimeter = delimeter;
        this.time = time;
        this.first_time = first_time;
        this.mysql = "1";
        this.deletes = deletes;
        this.ended = true;
    }
    public Task(){
    }
 
    @Override
    public void run() 
    {
        try {
        	if(this.ended == false) {
//        		System.out.println("Ignore");
        		return;
        	}

//    		System.out.println("Didnt Ignore");
			this.ended = false;
	    	MyThread th = new MyThread(this);
			th.start();
			th.join();

			if(this.first_time.equals("1")) {
				
				this.first_time = "0";
				int row = Integer.parseInt(this.taskID);
				int col = 14;
				// Read existing file 

				List<String[]> tasks = mainServ.getAllTasks();
				tasks.get(row-1)[col] = "0";

				FileWriter pw = new FileWriter("mytasks.csv",false); 
				for(String[] task: tasks) {
					String line = "";
					for(String tok: task) {
						line = line + tok + ";";
					}
					line = line.substring(0, line.length()-1);
					line = line + '\n';
					pw.append(line);
				}
				pw.close();
				
			}
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
