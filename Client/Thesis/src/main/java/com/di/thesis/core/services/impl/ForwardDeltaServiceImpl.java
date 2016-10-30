package com.di.thesis.core.services.impl;

import com.di.thesis.core.entities.Task;
import com.di.thesis.core.services.DeltaMakerService;
import com.di.thesis.core.services.ForwardDeltaService;

import java.io.*;
import java.net.Socket;
import java.sql.*;

public class ForwardDeltaServiceImpl implements ForwardDeltaService{

    private Task task;
    private boolean forward_success = false;

    public ForwardDeltaServiceImpl(Task task){
        this.task = task;
    }

    @Override
    public void forward() {

        Socket socket;
        String filename;
        DataInputStream in;
        DataOutputStream out;
        DeltaMakerService deltaMakerService = new DeltaMakeServiceImpl();

        if(task.get("is_mysql").equals("1")) {
            System.out.print("Creating CSV from table: " + task.get("name")+"... ");

            // Convert table to CSV
            tableToCsv();


            System.out.println("Keys: "+ task.get("unique_keys") );

            filename = System.getProperty("user.home") + "/" + task.get("db_name") +"." + task.get("name") + ".csv";

        } else {
            System.out.println("Creating DELTA from " + task.get("name"));
            filename = task.get("name");
        }


        Boolean deltaCreated = deltaMakerService.createDelta( filename, task.get("has_header"), task.get("unique_keys") , task.get("delimeter") );


        if(deltaCreated) {
            System.out.println("Delta created");

            while(!forward_success) {
                try {
                    System.out.println("Connecting to " + task.get("server_address") + " on port " + task.get("server_port"));

                    socket = new Socket(task.get("server_address"), Integer.parseInt(task.get("server_port")));
                    out = new DataOutputStream(socket.getOutputStream());

                    System.out.println("Just connected to " + socket.getRemoteSocketAddress());
                    System.out.println("============================================================");


                    File myFile = new File(filename + ".delta");
                    String filesize = String.valueOf((int)myFile.length());
                    String[] a = filename.split("/");
                    String name = a[a.length-1];
                    String info = name + "-" + task.get("delimeter") + "-" + task.get("has_header") + "-" + task.get("unique_keys") + "-" + task.get("first_time") + "-" + task.get("keep_deletes");

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

                    System.out.println("Bytes sent: " + c);

                    in = new DataInputStream(socket.getInputStream());

                    String ans = in.readUTF();

                    if(ans.equals("ENDED"))
                        forward_success = true;

                    System.out.println("Ended");
                    System.out.println("#####################");

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
        }
        else {
            System.out.println("No differences found");
        }
    }

    private void tableToCsv(){

        try {
            String connectionURL = "jdbc:mysql://"+ task.get("db_address") +"/" + task.get("db_name");
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection connection = (Connection) DriverManager.getConnection(connectionURL, task.get("username"), task.get("password"));


            // Get table Unique Keys
            String unique = "SHOW KEYS FROM " + task.get("name");
            Statement stmt = (Statement) connection.createStatement();
            ResultSet rs = stmt.executeQuery(unique);

            String keys = "";
            while (rs.next()) {
                String colname = rs.getString("Column_name");
                keys += colname + task.get("delimeter");
            }
            keys = keys.substring(0, keys.length()-1);

            task.put("unique_keys", keys);

            // Get all rows from the table
            String query = "select * from " + task.get("name");
            stmt = (Statement) connection.createStatement();
            rs = stmt.executeQuery(query);


            // Get column names of the table
            ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            String header = "";
            for (int i = 1; i <= columnCount; i++ ) {
                String col = rsmd.getColumnName(i);
                header += col + task.get("delimeter");
            }
            header = header.substring(0, header.length()-1);
            task.put("has_header", "1");

            String filename = System.getProperty("user.home") + "/" + task.get("db_name") +"." + task.get("name") + ".csv";
            PrintWriter out = new PrintWriter(filename);

            // Prtint the header to csv
            out.print(header);
            out.print('\n');


            while (rs.next()) {
                String col = "";
                String row = "";
                for (int i = 1; i <= columnCount; i++ ) {
                    if(rs.getBytes(i)==null)
                        col = "";
                    else
                        col = new String(rs.getBytes(i));

                    row +=  col + task.get("delimeter");
                }
                row = row.substring(0, row.length()-1);

                out.print(row);
                out.print('\n');
            }

            System.out.println("CSV File from Table is successfully created.");

            out.flush();
            out.close();
            connection.close();

        } catch (SQLException | FileNotFoundException | ClassNotFoundException |
                InstantiationException | IllegalAccessException ex) {
            ex.printStackTrace();
        }

    }
}
