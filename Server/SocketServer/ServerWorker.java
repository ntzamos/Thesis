import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;

public class ServerWorker implements Runnable{
	
	Socket server;
	private static DataInputStream in;
	private static DataOutputStream out;
	
	public ServerWorker(Socket server) {
		this.server = server;
        System.out.println("Just connected to " + this.server.getRemoteSocketAddress());
	}
	
	public void run() {
		try {
			in = new DataInputStream(server.getInputStream());
	
	        // Read Info from InputStream
	        String info = in.readUTF();
	        // Read Filesize from InputStream
	        String filesize = in.readUTF();
	
			System.out.println(info);
			System.out.println("Read file size: " + filesize);
	
			String[] info_parts = info.split("-");
			String filename = info_parts[0];
			String delimeter = info_parts[1];
			String has_header = info_parts[2];
			String unique_keys = info_parts[3];
			String first_time = info_parts[4];
			String mark_delete_option = info_parts[5];
			
			System.out.println("Filename: " + filename);
			System.out.println("Delimeter: " + delimeter);
			System.out.println("Has Header: " + has_header);
			System.out.println("Unique Keys: " + unique_keys);
			System.out.println("First Time: " + first_time);
			System.out.println("Mark Deletes: " + mark_delete_option);
	        System.out.println("============================================================");
	
//	        receiveFile from client
	        receiveFile(filename, filesize);
	        
	        // At first time just store Delta using the filename to HDFS
	    	if (first_time.equals("1")){
	    		System.out.println("File does not exist, Saving file as delta..");
	    		Process p7 = Runtime.getRuntime().exec("/home/cluster/hadoop-2.7.1/bin/hdfs dfs -mkdir /hdfs/inputs" + filename);
	    		p7.waitFor();
	    		Process p1 = Runtime.getRuntime().exec("/home/cluster/hadoop-2.7.1/bin/hdfs dfs -copyFromLocal /home/cluster/ptyxiaki/delta" + filename + " /hdfs/inputs" + filename + "/" + filename);
			 	p1.waitFor();
	    	}
	    	// FileToUpdate exists in HDFS
	    	else {
	    		System.out.println("File exists, Saving delta and updating file..");
	    		// Store DeltaFile to HDFS (using -f to overwrite)
			 	Process p2 = Runtime.getRuntime().exec("/home/cluster/hadoop-2.7.1/bin/hdfs dfs -copyFromLocal -f /home/cluster/ptyxiaki/delta" + filename +" /hdfs");
			 	p2.waitFor();
	
			 	// Execute Spark Application in order to update the file
			 	Process p3 = Runtime.getRuntime().exec("/home/cluster/spark-1.5.2-bin-hadoop2.6/bin/spark-submit "
			 			+ "--class com.incrementalupdates.app.App "
			 			+ "--master mesos://88.197.53.196:5050 "
			 			+ "/home/cluster/ptyxiaki/my-app/target/my-app-1.0-SNAPSHOT.jar " + filename + " " + has_header + " " + unique_keys + " " + delimeter + " " + mark_delete_option);
	
			 	p3.waitFor();
	
	    	}
	    	System.out.println("DONE");
	    	
	    	out = new DataOutputStream(server.getOutputStream());
	        out.writeUTF("ENDED");
	        out.flush();
		} 
		catch(IOException e){
			e.printStackTrace();
		}
		catch(InterruptedException e){
			e.printStackTrace();
		}
		
	}
	
	public static void receiveFile (String filename, String filesize) throws IOException{
    	int count = 0;
        byte[] buffer = new byte[Integer.valueOf(filesize)];
        FileOutputStream fos = new FileOutputStream("/home/cluster/ptyxiaki/delta" + filename);
        
        
        System.out.println("START");
        
        while ((count = in.read(buffer)) >= 0) {
        	System.out.println(count);
        	fos.write(buffer, 0, count);
        }
        fos.flush();
        System.out.println("END");
        fos.close();
    }
}
