package root;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Deltamaker {

	public static boolean createDelta(String file, String has_header, String keys) throws IOException, InterruptedException {

		String[] keysArray = keys.split(",");
		Integer[] keysIndex = new Integer[keysArray.length];


		String file_back = file + ".back";

		String deltaFile = file + ".delta";

		File f = new File(file_back);
		if(!f.exists()) {

			FileInputStream fstream = new FileInputStream(file);
			BufferedReader br_file = new BufferedReader(new InputStreamReader(fstream));

			PrintWriter out = new PrintWriter(deltaFile);

			String fileLine = br_file.readLine();
			int cnt=0;

			while (fileLine != null) {
				if(cnt == 0 && has_header.equals("1")) out.println(fileLine);
				else out.println(fileLine + ":0");

				cnt++;
				fileLine = br_file.readLine();
			}

			Process p = Runtime.getRuntime().exec("cp -f " + file + " " + file_back);
			p.waitFor();
			out.close();
			br_file.close();
			return true;
		}


		FileInputStream fstream = new FileInputStream(file);
		BufferedReader br_file = new BufferedReader(new InputStreamReader(fstream));

		FileInputStream fstreamback = new FileInputStream(file_back);
		BufferedReader br_back = new BufferedReader(new InputStreamReader(fstreamback));

		if(has_header.equals("1")) {
			br_back.readLine();
			String header = br_file.readLine();

			String[] headerArray = header.split(",");
			for(int i=0;i<keysArray.length;i++)
				for(int j=0;j<headerArray.length;j++)
					if(keysArray[i].equals(headerArray[j])) {
						keysIndex[i] = j;
						break;
					}

		} else {
			for(int i=0;i<keysArray.length;i++) keysIndex[i] = Integer.parseInt(keysArray[i]);
		}


		List<String> inserted_list = new ArrayList<String>();
		List<String> updated_list = new ArrayList<String>();
		List<String> deleted_list = new ArrayList<String>();

		// Start of files
		String fileLine = br_file.readLine();
		String backLine = br_back.readLine();

		// Parse files
		while (fileLine != null && backLine != null) {
			String keyF = "";
			String[] lineF = fileLine.split(",");
			for(int i=0;i<keysIndex.length;i++)
				keyF += lineF[keysIndex[i]];

			String keyB = "";
			String[] lineB = backLine.split(",");
			for(int i=0;i<keysIndex.length;i++) keyB += lineB[keysIndex[i]];

			if(keyF.equals(keyB)) {
				if(!fileLine.equals(backLine))
					updated_list.add(fileLine);
				fileLine = br_file.readLine();
				backLine = br_back.readLine();
			} else {
				deleted_list.add(backLine);
				backLine = br_back.readLine();
			}
		}

		while (fileLine != null) {
			inserted_list.add(fileLine);
			fileLine = br_file.readLine();
		}
		while (backLine != null) {
			deleted_list.add(backLine);
			backLine = br_back.readLine();
		}

		PrintWriter out = new PrintWriter(deltaFile);
		for (String temp : inserted_list) out.println("I:"+ temp);
		for (String temp : updated_list) out.println("U:"+ temp);
		for (String temp : deleted_list) out.println("D:"+ temp);

		out.close();
		br_file.close();
		br_back.close();

		Process p = Runtime.getRuntime().exec("cp -f " + file + " " + file_back);
		p.waitFor();

		return inserted_list.size() > 0 || updated_list.size() > 0 || deleted_list.size() > 0;
	}
}
