package root;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;


@WebServlet("/StopTaskServ")
public class StopTaskServ extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public StopTaskServ() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */

	public void StopTaskDB(String id) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		Connection connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
		Statement stmt = (Statement) connection.createStatement();
				 
		String sql = "UPDATE tasks SET active=0 WHERE id="+ id;
		stmt.executeUpdate(sql);
		connection.close();		
	}
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String id = request.getParameter("id");
		
		try {
			StopTaskDB(id);
			mainServ.tasks.get(id).cancel(false);
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

		response.sendRedirect("index.jsp");
		//doGet(request, response);
	}

}
