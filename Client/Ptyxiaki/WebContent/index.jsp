<%@ page import="java.sql.*" %> 
<%@ page import="java.io.*" %> 
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <meta name="description" content="">
    <meta name="author" content="">
    <link rel="icon" href="../../favicon.ico">
    <title>Ptyxiakara</title>

    <link href="/Ptyxiaki/css/bootstrap.min.css" rel="stylesheet">
    <link href="/Ptyxiaki/css/starter-template.css" rel="stylesheet">

  </head>

  <body>

    <nav class="navbar navbar-inverse navbar-fixed-top">
      <div class="container">
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="#">Task Manager</a>
        </div>
        <div id="navbar" class="collapse navbar-collapse">
          <ul class="nav navbar-nav">
            <li class="active"><a href="#">Home</a></li>
            <li><a href="#about">About</a></li>
          </ul>
        </div><!--/.nav-collapse -->
      </div>
    </nav>

    <div class="container">

      <div class="starter-template">
        <h1>Task Manager</h1>
        <p class="lead">Add new task or stop a current one.</p>
      </div>

    </div><!-- /.container -->
	
    <div class="container">
    <div id="myModal" class="modal fade" role="dialog">
	  <div class="modal-dialog">
	
	    <!-- Modal content-->
	    <div class="modal-content">
	      <div class="modal-header">
	        <button type="button" class="close" data-dismiss="modal">&times;</button>
	        <h4 class="modal-title">Add new Task</h4>
	      </div>
	      <div class="modal-body">
			  <div class="form-group">
			    <label for="name">Has Header </label>
			    <input type="checkbox" id="has_header" name="has_header" checked="checked">
			  </div>
			<form action="mainServ" method="POST">
			  <div class="form-group">
			    <label for="name">Filename</label>
			    <input type="text" class="form-control" id="filename" name="filename" placeholder="Filename">
			  </div>
			  <div class="form-group">
			    <label for="name">Server Address</label>
			    <input type="text" class="form-control" id="server_address" name="server_address" placeholder="Server Address">
			  </div>
			  <div class="form-group">
			    <label for="name">Delimeter</label>
			    <input type="text" class="form-control" id="delimeter" name="delimeter" placeholder="Delimeter">
			  </div>
			  <div class="form-group">
			    <label for="name">Unique Keys (comma separated)</label>
			    <input type="text" class="form-control" id="unique_keys" name="unique_keys" placeholder="key1,key2,key3,..">
			  </div>
			  <div class="form-group">
			    <label for="seconds">Repeat Every Seconds</label>
			    <input type="text" class="form-control" id="time" name="time" placeholder="Seconds">
			  </div>
			  <button type="submit" class="btn btn-info btn-default">Create Task</button>
			</form>
	      </div>
	      <div class="modal-footer">
	        <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
	      </div>
	    </div>
	
	  </div>
	</div>
	<button type="button" class="btn btn-info btn-lg" data-toggle="modal" data-target="#myModal">Add new Task</button>
	<hr/>

		<!-- 
		<form action="taskServ" method="POST">
		<input type="hidden" value="1" name="val">
		Task id: <input type="text" name="id">  <input type="submit" value="Start" />
		</form> -->
	
	<table class="table">
		<caption>My Tasks</caption>
		<thead> <tr> <th>Header</th> <th>Filename</th> <th>Server Address</th> <th>Delimeter</th> <th>Unique Keys</th> <th>Seconds</th></tr> </thead>
		<tbody> 
		<% 
	try {
	    String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
	    Connection connection = null; 
	    Class.forName("com.mysql.jdbc.Driver").newInstance(); 
	    connection = DriverManager.getConnection(connectionURL, "root", "root");
	    
	    Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM tasks WHERE active = 1");
		while (rs.next()) {
			Integer has_header = rs.getInt("has_header");
			String id = rs.getString("id");
			String filename = rs.getString("filename");
			String address = rs.getString("server_address");
			String delimeter = rs.getString("delimeter");
			String keys = rs.getString("unique_keys");
			String time = rs.getString("time");
			String active = rs.getString("active");
		    out.println("<tr> <th scope='row'>"+ has_header +"</th> <td>"+filename+"</td>  <td>"+address+"</td>  <td>"+delimeter+"</td> <td>"+keys+"</td> <td>"+time+"</td> <td>"+	
		    "<form action='taskServ' method='POST'><input type='hidden' value='0' name='val'>"+
			"<input type='hidden' name='id' value='"+ id +"'>  <input type='submit' value='STOP' /></form></td></tr>");
		}
	    connection.close();
	}catch(Exception ex){
	    out.println("Unable to connect to database.");ex.printStackTrace();
	}
	%>
	 </tbody>
	</table>
</div>
    <script src="/Ptyxiaki/js/jquery.min.js"></script>
    <script src="/Ptyxiaki/js/bootstrap.min.js"></script>

  </body>
</html>

