
<%@ page import="root.*" %> 
<%@ page import="java.sql.*" %> 
<%@ page import="java.io.*" %> 
<%@ page import="java.util.*" %> 
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
	        <h4 class="modal-title">Add new CSV Task</h4>
	      </div>
	      <div class="modal-body">
			<form action="mainServ" method="POST">
				<input type="hidden" class="form-control"  id="mysql" name="mysql" value="0">
				<input type="hidden" class="form-control"  id="csv" name="csv" value="1">
			  <div class="form-group">
			    <label for="name">Has Header </label>
			    <input type="text" class="form-control"  id="has_header" name="has_header" value="0" placeholder="0">
			  </div>
			  <div class="form-group">
			    <label for="name">File Full path</label>
			    <input type="text" class="form-control" id="filename" name="filename" placeholder="Filename">
			  </div>
			  <div class="form-group">
			    <label for="name">Server Address</label>
			    <input type="text" class="form-control" id="server_address" name="server_address" placeholder="Server Address">
			  </div>
			  <div class="form-group">
			    <label for="name">Server Port</label>
			    <input type="text" class="form-control" id="server_port" name="server_port" placeholder="Server Port">
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
	<div id="myModalSQL" class="modal fade" role="dialog">
	  <div class="modal-dialog">
	
	    <!-- Modal content-->
	    <div class="modal-content">
	      <div class="modal-header">
	        <button type="button" class="close" data-dismiss="modal">&times;</button>
	        <h4 class="modal-title">Add new MySQL Task</h4>
	      </div>
	      <div class="modal-body">
			<form action="mainServ" method="POST">
				<input type="hidden" class="form-control"  id="mysql" name="mysql" value="1">
				<input type="hidden" class="form-control"  id="csv" name="csv" value="0">
			  
			  <div class="form-group">
			    <label for="database_address">Database Address</label>
			    <input type="text" class="form-control" name="database_address" placeholder="Database Address">
			  </div>
			  <div class="form-group">
			    <label for="database">Database</label>
			    <input type="text" class="form-control" name="database" placeholder="Database">
			  </div>
			  
			  <div class="form-group">
			    <label for="tablename">Table name</label>
			    <input type="text" class="form-control" name="tablename" placeholder="Table name">
			  </div>
			  <div class="form-group">
			    <label for="username">Username</label>
			    <input type="text" class="form-control" name="username" placeholder="Username">
			  </div>
			  <div class="form-group">
			    <label for="password">Password</label>
			    <input type="text" class="form-control" name="password" placeholder="Password">
			  </div>
			  <hr/>
			  <div class="form-group">
			    <label for="server_address">Server Address</label>
			    <input type="text" class="form-control" id="server_address" name="server_address" placeholder="Server Address">
			  </div>
			  <div class="form-group">
			    <label for="server_port">Server Port</label>
			    <input type="text" class="form-control" id="server_port" name="server_port" placeholder="Server Port">
			  </div>
			  <div class="form-group">
			    <label for="delimeter">Delimeter</label>
			    <input type="text" class="form-control" id="delimeter" name="delimeter" placeholder="Delimeter">
			  </div>
			  
			  <div class="form-group">
			    <label for="deletes">Keep deletes or not</label>
			    <input type="text" class="form-control" id="deletes" name="deletes" placeholder="1">
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
	<button type="button" class="btn btn-info btn-lg" data-toggle="modal" data-target="#myModal">Add new CSV Task</button>
	<button type="button" class="btn btn-info btn-lg" data-toggle="modal" data-target="#myModalSQL">Add new MySQL Task</button>
	<hr/>

	<table class="table">
		<caption>My CSV Tasks</caption>
		<thead> <tr> <th>Header</th> <th>Filename</th> <th>Server Address</th> <th>Server Port</th> <th>Delimeter</th> <th>Unique Keys</th> <th>Seconds</th></tr> </thead>
		<tbody> 
		<% 

		List<String[]> tasks = mainServ.getAllTasks();
		for(String[] tokens: tasks) {

		 	if(tokens[1].equals("0"))
		    out.println("<tr> <th scope='row'>"+ tokens[12] +"</th> <td>"+tokens[7]+"</td>  <td>"+tokens[8]+"</td>  <td>"+tokens[9]+"</td>  <td>"+tokens[10]+"</td> <td>"+tokens[13]+"</td> <td>"+tokens[15]+"</td> <td>"+	
				    "<form action='StopTaskServ' method='POST'>"+
					"<input type='hidden' name='id' value='"+ tokens[0] +"'>  <input type='submit' value='STOP' /></form></td></tr>");
				
		}
	%>
	 </tbody>
	</table>
	<br/>
	<table class="table">
		<caption>My MySQL Tasks</caption>
		<thead> <tr> <th>Database</th> <th>Tablename</th> <th>Server Address</th> <th>Server Port</th> <th>Delimeter</th> <th>Seconds</th></tr> </thead>
		<tbody> 
		<% 

			List<String[]> sqltasks = mainServ.getAllTasks();
			for(String[] tokens: sqltasks) {
			 	if(tokens[1].equals("1"))
			
			    out.println("<tr> <th scope='row'>"+ tokens[3] +"</th> <td>"+tokens[4]+"</td>  <td>"+tokens[8]+"</td>  <td>"+tokens[9]+"</td>  <td>"+tokens[10]+"</td> <td>"+tokens[15]+"</td> <td>"+	
			    "<form action='StopTaskServ' method='POST'>"+
				"<input type='hidden' name='id' value='"+ tokens[0] +"'>  <input type='submit' value='STOP' /></form></td></tr>");
			}

	%>
	 </tbody>
	</table>
</div>
    <script src="/Ptyxiaki/js/jquery.min.js"></script>
    <script src="/Ptyxiaki/js/bootstrap.min.js"></script>

  </body>
</html>

