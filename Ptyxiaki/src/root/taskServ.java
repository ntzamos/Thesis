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


@WebServlet("/taskServ")
public class taskServ extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public taskServ() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */

	public void updateDB(String id, String val) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String connectionURL = "jdbc:mysql://localhost/ptyxiaki";
		Connection connection = null; 
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		connection = (Connection) DriverManager.getConnection(connectionURL, "root", "root");
		
		Statement stmt = (Statement) connection.createStatement();
		
		 
		String sql = "UPDATE tasks SET active="+val+" WHERE id="+ id;
		stmt.executeUpdate(sql);
		
		connection.close();
		
		
	}
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String id = request.getParameter("id");
		String val = request.getParameter("val");
		try {
			updateDB(id,val);
			if(val.equals("0")) mainServ.tasks.get(id).cancel(false);
			
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

		response.sendRedirect("index.jsp");
		//doGet(request, response);
	}

}
