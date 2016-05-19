package root;
import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

class MyThread extends Thread {

    Task task;
    DataOutputStream out;
    
    public MyThread(Task task) {
    	this.task = task;
    }
    
    @Override
    public void run() {
		try {

		    System.out.println("Creating DELTA");
	        if(Deltamaker.createDelta(task.filename, task.has_header, task.unique_keys)) { 
	        	System.out.println("Delta created");
	        	
			    System.out.println("Connecting to " + task.address + " on port " + task.server_port);
			    Socket client = new Socket(task.address, Integer.parseInt(task.server_port));
			    System.out.println("Just connected to " + client.getRemoteSocketAddress());
			    System.out.println("============================================================");

			    OutputStream outToServer = client.getOutputStream();
			    out = new DataOutputStream(outToServer);
			    File myFile = new File(task.filename + ".delta");
			    String filesize = String.valueOf((int)myFile.length());
	            String[] a = task.filename.split("/");
	            String filename = a[a.length-1];
			    String info = filename + "-" + task.delimeter + "-" + task.has_header + "-" + task.unique_keys + "-" + task.first_time;
	            
			    System.out.println("Sending file length: " + filesize);
			       
			    out.writeUTF(info);
			    out.writeUTF(filesize);
       

			    int count;
			    byte[] buffer = new byte[Integer.valueOf(filesize)];
			
			    outToServer = client.getOutputStream();
			    BufferedInputStream in = new BufferedInputStream(new FileInputStream(myFile));
			    while ((count = in.read(buffer)) >= 0) out.write(buffer, 0, count);
			    out.flush();
			       
			    in.close();
			    client.close();

			    System.out.println("File sent: " + filesize);
	        }
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}