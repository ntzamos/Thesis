package root;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;

class MyThread extends Thread {

   // private String cmd;
    private String filename;

    static BufferedReader bufIn;
    static BufferedWriter bufOut;
    static OutputStream out;
    public MyThread(String cmd, String filename) {
        //this.cmd = cmd;
        this.filename = filename;
    }
    public static boolean sendStr(String str) throws IOException {
    	bufOut.write( str );
    	bufOut.newLine();
        bufOut.flush();
    	return true;
    }
    public static void sendFile(String filename) throws IOException {
    	File file = new File(filename);
        long length = file.length();
        byte[] bytes = new byte[(int) length];
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);

        int count;

        while ((count = bis.read(bytes)) > 0) {
            out.write(bytes, 0, count);
        }

        bis.close();
    	
    }
    @Override
    public void run() {


		try {

			@SuppressWarnings("resource")
			Socket socket = new Socket("127.0.0.1", 1993);
			OutputStream os = socket.getOutputStream();
			out = new BufferedOutputStream(os);
	        bufOut = new BufferedWriter(new OutputStreamWriter(out));
	        
	        sendStr(filename);
	        
	        sendFile("/home/commando/init");
	        
	        bufOut.close();
	        out.close();
	        os.close();
	        
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}