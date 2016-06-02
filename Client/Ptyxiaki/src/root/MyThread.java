package root;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.DriverManager;
import java.sql.ResultSet;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetMetaData;
import com.mysql.jdbc.Statement;

class MyThread extends Thread {

    Task task;
    DataOutputStream out;
    DataInputStream in;
    Socket socket;
    
    public MyThread(Task task) {
    	this.task = task;
    }
    private String createCSV() {
    	String filename = "/home/commando/"+task.db +"."+task.table+".csv";
    	//String filename = "/home/cluster/"+task.db +"."+task.table+".csv";
    	task.filename = filename;
    	String ret = "";
            try {

      	    	PrintWriter out = new PrintWriter(filename);
                String connectionURL = "jdbc:mysql://"+ task.dbaddress +"/" + task.db;
        		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
        		Connection connection = (Connection) DriverManager.getConnection(connectionURL, task.user, task.pass);
        		
        		
        		//GET KEYS
        		String unique = "SHOW KEYS FROM " + task.table;
                Statement stmt = (Statement) connection.createStatement();
                ResultSet rs = stmt.executeQuery(unique);

                while (rs.next()) {
                	String colname = rs.getString("Column_name");   
                	ret += colname +",";
                }
                ret = ret.substring(0, ret.length()-1);
                
                //GET ROWS
                String query = "select * from " + task.table;
                stmt = (Statement) connection.createStatement();
                rs = stmt.executeQuery(query);

            	ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();
            	int columnCount = rsmd.getColumnCount();
            	//GET HEADER
            	String col = "";
            	for (int i = 1; i < columnCount; i++ ) {
        		  col = rsmd.getColumnName(i);
        		  out.print(col);
        		  out.print(',');
        		}
            	 col = rsmd.getColumnName(columnCount);
            	  out.print(col);
            	  out.print('\n');
            	
            	//GET RECORDS
                while (rs.next()) {
                	for (int i = 1; i < columnCount; i++ ) {
                		if(rs.getBytes(i)==null) col = "";
                		else col = new String(rs.getBytes(i));
            		  out.print(col);
            		  out.print(',');
            		}
            		if(rs.getBytes(columnCount)==null) col = "";
            		else col = new String(rs.getBytes(columnCount));
	        		  out.print(col);
	        		  out.print('\n');
                }

    			out.flush();
    			out.close();
                connection.close();
                System.out.println("CSV File is created successfully.");
            } catch (Exception e) {
                e.printStackTrace();
            }
			return ret;
    	
    }
    @Override
    public void run() {
		    System.out.println("");

		    System.out.println("#################################");
			
			if(task.mysql.equals("1")) {
			    System.out.print("Creating CSV from table: "+task.table+"... ");
				String keys = createCSV();

			    task.delimeter = ",";
			    task.has_header = "1";
			    task.unique_keys = keys;
			    
			    System.out.println("Keys: "+ keys);
			    
			} 
		    System.out.println("Creating DELTA from "+ task.filename);
			if(Deltamaker.createDelta( task.filename, task.has_header, task.unique_keys)) { 
				System.out.println("Delta created");

					while(!task.ended) {
						try {
						    System.out.println("Connecting to " + task.address + " on port " + task.server_port);
						    
							socket = new Socket(task.address, Integer.parseInt(task.server_port));
						    out    = new DataOutputStream(socket.getOutputStream());

						    System.out.println("Just connected to " + socket.getRemoteSocketAddress());
						    System.out.println("============================================================");
						    
						    
						    File myFile = new File(task.filename + ".delta");
						    String filesize = String.valueOf((int)myFile.length());
					        String[] a = task.filename.split("/");
					        String filename = a[a.length-1];
						    String info = filename + "-" + task.delimeter + "-" + task.has_header + "-" + task.unique_keys + "-" + task.first_time + "-" + task.deletes;
					        
						    System.out.println("Sending file length: " + filesize);
						       
						    out.writeUTF(info);
						    out.writeUTF(filesize);
	      
	
						    int count;
						    int c = 0;
						    byte[] buffer = new byte[Integer.valueOf(filesize)];
						    BufferedInputStream fin = new BufferedInputStream(new FileInputStream(myFile));
						    while ((count = fin.read(buffer)) >= 0) {
						    	out.write(buffer, 0, count);
						    	c += count;
						    }
						    out.flush();
						    socket.shutdownOutput();

//						    System.out.println("Bytes sent: " + c);

						    in = new DataInputStream(socket.getInputStream());

						    String ans = in.readUTF();
						    
						    if(ans.equals("ENDED")) 
						    	task.ended = true;

						    System.out.println("Ended");
						    System.out.println("###########");

						    fin.close();
						    in.close();
						    out.close();
						    socket.close();
					    	break;

						} catch (IOException e) {
							e.printStackTrace();
						} 
						
						try {

						    System.out.println("Retrying to connect in 5 seconds");
							Thread.sleep(5000);
							
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

			    
			} else {
				System.out.println("No differences found");
				task.ended = true;
			}
				
	

    }
}