package root;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



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
	 * @throws IOException 
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */

	public void StopTaskDB(String id) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		int row = Integer.parseInt(id);
		int col = 16;
		// Read existing file 

		List<String[]> tasks = mainServ.getAllTasks();
		tasks.get(row-1)[col] = "0";

		FileWriter pw = new FileWriter("mytasks.csv",false); 
		for(String[] task: tasks) {
			String line = "";
			for(String tok: task) {
				line = line + tok + ";";
			}
			line = line.substring(0, line.length()-1);
			line = line + '\n';
			pw.append(line);
		}
		pw.close();
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
