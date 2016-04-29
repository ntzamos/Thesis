import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;


public class runClass {

	private static Socket socket;

    static BufferedReader bufIn;
    static BufferedWriter bufOut;

    public static void receiveFile() throws IOException {
        InputStream is = socket.getInputStream();
        int bufferSize = socket.getReceiveBufferSize();

        FileOutputStream fos = new FileOutputStream("/home/agg/init2");
        BufferedOutputStream bos = new BufferedOutputStream(fos);

        byte[] bytes = new byte[bufferSize];
        int count;
        while ((count = is.read(bytes)) > 0) {
            bos.write(bytes, 0, count);
        }



        bos.flush();
        bos.close();

    }
	public static void main(String[] args) throws IOException, InterruptedException {
		ServerSocket serverSocket = new ServerSocket(1993, 50);

		while(true) {
			
			System.out.println("======================");
			System.out.println("Start");
			System.out.println("======================");
			socket = serverSocket.accept();

			bufIn = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
			bufOut = new BufferedWriter( new OutputStreamWriter( socket.getOutputStream() ) );

			String info = bufIn.readLine();
			System.out.println(info);
			
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
			
			receiveFile();
			
			// At first time just store Delta using the filename to HDFS
			if (first_time.equals("1")){
				Process p1 = Runtime.getRuntime().exec("~/hadoop-2.7.1/bin/hadoop dfs -copyFromLocal delta /hdfs/" + filename);
				p1.waitFor();
			}
			// FileToUpdate exists in HDFS
			else {
				// Store DeltaFile to HDFS (using -f to overwrite)
				Process p2 = Runtime.getRuntime().exec("~/hadoop-2.7.1/bin/hadoop dfs -copyFromLocal -f delta /hdfs");
				p2.waitFor();
				
				// Execute Spark Application in order to update the file
				Process p3 = Runtime.getRuntime().exec("~/spark-1.5.2-bin-hadoop2.6/bin/spark-submit "
						+ "--class com.incrementalupdates.app.App "
						+ "--master mesos://88.197.53.192:5050 "
						+ "target/my-app-1.0-SNAPSHOT.jar " + filename + " " + has_header + " " + unique_keys + " " + delimeter);
		 
				p3.waitFor();
		
			}
			
			System.out.println("======================");
			System.out.println("End");
			System.out.println("======================");
		}
	}
}
