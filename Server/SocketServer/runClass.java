import java.io.*;
import java.net.ServerSocket;


public class runClass {

	private static ServerSocket serverSocket;

	public static void main(String[] args) throws IOException {

		Integer port = Integer.parseInt(args[0]);

		serverSocket = new ServerSocket(port);
		while(true)
		{
			ServerWorker worker;

			try{
				System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
	            worker = new ServerWorker(serverSocket.accept());

	            Thread th = new Thread(worker);
	            th.start();

			}catch(IOException e){
				e.printStackTrace();
			}
		}

	}
}
