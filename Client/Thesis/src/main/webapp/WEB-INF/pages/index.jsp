<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page import="java.util.List" %>
<%@ page import="com.di.thesis.core.entities.Task" %>
<%@ page import="com.di.thesis.core.listener.StartupShutdownListener" %>

<html>
<head>
    <title>Tasks Interface</title>

    <link href="<c:url value="/resources/bootstrap-3.3.7/bootstrap.min.css" />" rel="stylesheet">
    <link href="<c:url value="/resources/bootstrap-3.3.7/starter-template.css" />" rel="stylesheet">
    <script src="<c:url value="/resources/bootstrap-3.3.7/jquery.min.js" />"></script>
    <script src="<c:url value="/resources/bootstrap-3.3.7/bootstrap.min.js" />"></script>
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
                    <li><a href="#">About</a></li>
                </ul>
            </div>
        </div>
    </nav>

    <div class="container">
        <div class="starter-template">
            <h1>Task Manager</h1>
            <p class="lead">Add new task or stop a current one.</p>
        </div>
    </div>

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
                        <form action="createTask" method="POST">
                            <input type="hidden" class="form-control" name="is_mysql" value="0">
                            <div class="form-group">
                                <label for="has_header">Has Header </label>
                                <input type="text" class="form-control"  id="has_header" name="has_header" value="0" placeholder="0">
                            </div>
                            <div class="form-group">
                                <label for="name">File Full path</label>
                                <input type="text" class="form-control" id="name" name="name" placeholder="Filename">
                            </div>
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
                                <label for="unique_keys">Unique Keys (comma separated)</label>
                                <input type="text" class="form-control" id="unique_keys" name="unique_keys" placeholder="key1,key2,key3,..">
                            </div>
                            <div class="form-group">
                                <label for="keep_deletes">Keep deletes or not</label>
                                <input type="text" class="form-control" id="keep_deletes" name="keep_deletes" placeholder="1">
                            </div>
                            <div class="form-group">
                                <label for="time">Repeat Every Seconds</label>
                                <input type="text" class="form-control" id="time" name="time" placeholder="Seconds">
                            </div>
                            <button type="submit" class="btn btn-primary btn-default">Create Task</button>
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
                        <form action="createTask" method="POST">
                            <input type="hidden" class="form-control" name="is_mysql" value="1">

                            <div class="form-group">
                                <label for="db_address">Database Address</label>
                                <input type="text" class="form-control" id="db_address" name="db_address" placeholder="Database Address">
                            </div>
                            <div class="form-group">
                                <label for="db_name">Database</label>
                                <input type="text" class="form-control" id="db_name" name="db_name" placeholder="Database Name">
                            </div>

                            <div class="form-group">
                                <label for="tablename">Table name</label>
                                <input type="text" class="form-control" id="tablename" name="name" placeholder="Table name">
                            </div>
                            <div class="form-group">
                                <label for="username">Username</label>
                                <input type="text" class="form-control" id="username" name="username" placeholder="Username">
                            </div>
                            <div class="form-group">
                                <label for="password">Password</label>
                                <input type="password" class="form-control" id="password" name="password" placeholder="Password">
                            </div>
                            <hr/>
                            <div class="form-group">
                                <label for="server_address2">Server Address</label>
                                <input type="text" class="form-control" id="server_address2" name="server_address" placeholder="Server Address">
                            </div>
                            <div class="form-group">
                                <label for="server_port2">Server Port</label>
                                <input type="text" class="form-control" id="server_port2" name="server_port" placeholder="Server Port">
                            </div>
                            <div class="form-group">
                                <label for="delimeter2">Delimeter</label>
                                <input type="text" class="form-control" id="delimeter2" name="delimeter" placeholder="Delimeter">
                            </div>

                            <div class="form-group">
                                <label for="deletes">Keep deletes or not</label>
                                <input type="text" class="form-control" id="deletes" name="keep_deletes" placeholder="1">
                            </div>
                            <div class="form-group">
                                <label for="seconds">Repeat Every Seconds</label>
                                <input type="text" class="form-control" id="seconds" name="time" placeholder="Seconds">
                            </div>
                            <button type="submit" class="btn btn-primary">Create Task</button>
                        </form>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
                    </div>
                </div>

            </div>
        </div>

        <button type="button" class="btn btn-primary btn-lg" data-toggle="modal" data-target="#myModal">Add new CSV Task</button>
        <button type="button" class="btn btn-primary btn-lg" data-toggle="modal" data-target="#myModalSQL">Add new MySQL Task</button>
        <hr/>
        <table class="table">
            <caption>CSV Tasks</caption>
            <thead> <tr> <th>Header</th> <th>Filename</th> <th>Server Address</th> <th>Server Port</th> <th>Delimeter</th> <th>Unique Keys</th> <th>Seconds</th></tr> </thead>
            <tbody>
                <%
                List<Task> tasks = StartupShutdownListener.tasks.getTasks();
                for(Task t: tasks) {

                    if(t.get("is_mysql").equals("0") && t.get("active").equals("1")) { %>
                        <tr>
                            <th scope='row'><%=t.get("has_header")%></th>
                            <td><%=t.get("name")%></td>
                            <td><%=t.get("server_address")%></td>
                            <td><%=t.get("server_port")%></td>
                            <td><%=t.get("delimeter")%></td>
                            <td><%=t.get("unique_keys")%></td>
                            <td><%=t.get("time")%></td>
                            <td>
                                <form action='pauseTask' method='POST'>
                                    <input type='hidden' name='id' value=<%=t.get("id")%>>
                                    <input type='submit' class="btn btn-danger" value='STOP' />
                                </form>
                            </td>
                        </tr>
                    <%}%>
                    <%if(t.get("is_mysql").equals("0") && t.get("active").equals("0")) { %>
                        <tr style='text-decoration: line-through;'>
                            <th scope='row'><%=t.get("has_header")%></th>
                            <td><%=t.get("name")%></td>
                            <td><%=t.get("server_address")%></td>
                            <td><%=t.get("server_port")%></td>
                            <td><%=t.get("delimeter")%></td>
                            <td><%=t.get("unique_keys")%></td>
                            <td><%=t.get("time")%></td>
                            <td>
                                <form action='resumeTask' method='POST'>
                                    <input type='hidden' name='id' value=<%=t.get("id")%>>
                                    <input type='submit' class="btn btn-success" value='START' />
                                </form>
                            </td>
                        </tr>
                    <%}%>
                <%}%>
            </tbody>
        </table>

        <br/>
        <table class="table">
            <caption>MySQL Tasks</caption>
            <thead> <tr> <th>Database</th> <th>Tablename</th> <th>Server Address</th> <th>Server Port</th> <th>Delimeter</th> <th>Seconds</th></tr> </thead>
            <tbody>
                <%for(Task t: tasks) {
                    if(t.get("is_mysql").equals("1") && t.get("active").equals("1")) { %>
                        <tr>
                            <th scope='row'><%=t.get("db_name")%></th>
                            <td><%=t.get("name")%></td>
                            <td><%=t.get("server_address")%></td>
                            <td><%=t.get("server_port")%></td>
                            <td><%=t.get("delimeter")%></td>
                            <td><%=t.get("time")%></td>
                            <td>
                                <form action='pauseTask' method='POST'>
                                    <input type='hidden' name='id' value=<%=t.get("id")%>>
                                    <input type='submit' class="btn btn-danger" value='STOP' />
                                </form>
                            </td>
                        </tr>)
                    <%}%>

                    <% if(t.get("is_mysql").equals("1") && t.get("active").equals("0")) { %>
                        <tr style='text-decoration: line-through;'>
                            <th scope='row'><%=t.get("db_name")%></th>
                            <td><%=t.get("name")%></td>
                            <td><%=t.get("server_address")%></td>
                            <td><%=t.get("server_port")%></td>
                            <td><%=t.get("delimeter")%></td>
                            <td><%=t.get("time")%></td>
                            <td>
                                <form action='resumeTask' method='POST'>
                                    <input type='hidden' name='id' value=<%=t.get("id")%>>
                                    <input type='submit' class="btn btn-success" value='START' />
                                </form>
                            </td>
                        </tr>
                    <%}%>
                <%}%>
            </tbody>
        </table>
    </div>
</body>
</html>
