import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;


public class runClass {

	private static ServerSocket serverSocket;
	private static DataInputStream in;

    public static void receiveFile (String filesize) throws IOException{
    	int count;
        byte[] buffer = new byte[Integer.valueOf(filesize)];
        FileOutputStream fos = new FileOutputStream("/home/cluster/ptyxiaki/delta");

        while ((count = in.read(buffer)) >= 0) 
    		fos.write(buffer, 0, count);

        fos.close();
    }

	public static void main(String[] args) throws IOException, InterruptedException {

		serverSocket = new ServerSocket(19933);
		while(true)
		{
			try
			{

				System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
	            Socket server = serverSocket.accept();
	            System.out.println("Just connected to " + server.getRemoteSocketAddress());

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

				System.out.println("Filename: " + filename);
				System.out.println("Delimeter: " + delimeter);
				System.out.println("Has Header: " + has_header);
				System.out.println("Unique Keys: " + unique_keys);
				System.out.println("First Time: " + first_time);
	            System.out.println("============================================================");

	            // receiveFile from client
	            receiveFile(filesize);

	            // At first time just store Delta using the filename to HDFS
            	if (first_time.equals("1")){
            		System.out.println("File does not exist, Saving file as delta..");

            		Process p1 = Runtime.getRuntime().exec("/home/cluster/hadoop-2.7.1/bin/hdfs dfs -copyFromLocal /home/cluster/ptyxiaki/delta /hdfs/inputs/" + filename);
				 	p1.waitFor();
            	}
            	// FileToUpdate exists in HDFS
            	else {
            		System.out.println("File exists, Saving delta and updating file..");
            		// Store DeltaFile to HDFS (using -f to overwrite)
				 	Process p2 = Runtime.getRuntime().exec("/home/cluster/hadoop-2.7.1/bin/hdfs dfs -copyFromLocal -f /home/cluster/ptyxiaki/delta /hdfs");
				 	p2.waitFor();

				 	// Execute Spark Application in order to update the file
				 	Process p3 = Runtime.getRuntime().exec("/home/cluster/spark-1.5.2-bin-hadoop2.6/bin/spark-submit "
				 			+ "--class com.incrementalupdates.app.App "
				 			+ "--master mesos://88.197.53.192:5050 "
				 			+ "/home/cluster/ptyxiaki/my-app/target/my-app-1.0-SNAPSHOT.jar " + filename + " " + has_header + " " + unique_keys + " " + delimeter + " > /home/cluster/ptyxiaki/results");

				 	p3.waitFor();

            	}

	            server.close();

			}catch(IOException e){
				e.printStackTrace();
				break;
			}
		}

		System.out.println("======================");
		System.out.println("End");
		System.out.println("======================");
	}
}
