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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Deltamaker {

	public static boolean createDelta(String file, String has_header, String keys) throws NoSuchAlgorithmException, IOException {
		
		String[] keysArray = keys.split(",");
		Integer[] keysIndex = new Integer[keysArray.length];
		
		String file_back = file + ".back";
		String deltaFile = file + ".delta";
		
		List<String> inserted_list = new ArrayList<String>();	
		List<String> updated_list = new ArrayList<String>();	
		List<String> deleted_list = new ArrayList<String>();
		
		CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache("preConfigured",
		               CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(100))
		               .build()).build(true);
		
		Cache<String, String> preConfigured = cacheManager.getCache("preConfigured", String.class, String.class);
		preConfigured.put("nikos", "da one!");
		
	    String value = preConfigured.get("nikos");
	    System.out.println(value);
	      
		// CheckSum for the lines
		MessageDigest md = MessageDigest.getInstance("MD5");
		
        File backup = new File(file_back);
        
        if(!backup.exists()) {

    		FileInputStream fstream = new FileInputStream(file);
    		BufferedReader br_file = new BufferedReader(new InputStreamReader(fstream));

  	    	PrintWriter out = new PrintWriter(deltaFile);
  	    	
    		String fileLine = br_file.readLine();
    		int cnt=0;
    		
			while (fileLine != null) {
				if(cnt == 0 && has_header.equals("1")) 
					out.println(fileLine);
				else 
					out.println(fileLine + ":0");
				
				cnt++;
				fileLine = br_file.readLine(); 
			}

	        try {
	        	Process p = Runtime.getRuntime().exec("cp -f " + file + " " + file_back);
				p.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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
			
			for(int i = 0; i < keysArray.length; i++) 
				for(int j = 0; j < headerArray.length; j++) 
					if(keysArray[i].equals(headerArray[j])) {
						keysIndex[i] = j;
						break;
					}
			
		} else {
			for(int i = 0; i < keysArray.length; i++) 
				keysIndex[i] = Integer.parseInt(keysArray[i]);			
		}

		
		// Map Current File (unique key, line)
		Map<String, String> mapCur = new HashMap<String, String>();
		
		String fileLine = br_file.readLine();
		
		while (fileLine != null) {
			String key = "";
			String[] line = fileLine.split(",");
			
			for(int i = 0; i < keysIndex.length; i++) {
				if (i == keysIndex.length - 1)
					key += line[keysIndex[i]];
				else
					key += line[keysIndex[i]] + ",";
			}
			byte[] bytesOfMessage = fileLine.getBytes("UTF-8");
			byte[] thedigest = md.digest(bytesOfMessage);
			
			// convert byte array to Hex String
			StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < thedigest.length; i++)
	        	sb.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16).substring(1));
	        
	        // StringBuffer to String
	        String value_checksum = sb.toString();
	        
			// Add key - value pair
			mapCur.put(key, value_checksum);
			
			fileLine = br_file.readLine(); 
		}
		
		// Map Backup File (unique key, line)
		Map<String, String> mapBack = new HashMap<String, String>();
		
		String backLine = br_back.readLine();

		while (backLine != null) {
			String key = "";
			String[] line = backLine.split(",");
	    	  
			for(int i = 0; i < keysIndex.length; i++) {
				if (i == keysIndex.length - 1)
					key += line[keysIndex[i]];
				else
					key += line[keysIndex[i]] + ",";
			}
	    	  
			byte[] bytesOfMessage = backLine.getBytes("UTF-8");
			byte[] thedigest = md.digest(bytesOfMessage);
			
			// convert byte array to Hex String
			StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < thedigest.length; i++)
	        	sb.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16).substring(1));
	        
	        // StringBuffer to String
	        String value_checksum = sb.toString();
	        
			// Add key - value pair
	        mapBack.put(key, value_checksum);
	    	  
			// Continue to the next line
			backLine = br_back.readLine(); 	
		}
	      
	    System.out.println("PRINTING CURRENT FILE MAP");
	    for (Map.Entry<String,String> curfile_map : mapCur.entrySet()){
	    	String key = curfile_map.getKey();
			String val = curfile_map.getValue();
			System.out.println("Key: " + key + " Value: " + val);
	    }
	    
	    System.out.println("PRINTING BACKUP FILE MAP");
	    for (Map.Entry<String,String> backup_map : mapBack.entrySet()){
	    	String key = backup_map.getKey();
			String val = backup_map.getValue();
			System.out.println("Key: " + key + " Value: " + val);
	    }
	    
	    // For every entry of the current file
		for (Map.Entry<String,String> entry : mapCur.entrySet()) {
			
			String key = entry.getKey();	// get the Unique key
			String val = entry.getValue();	// get the Line
			
			String line = mapBack.get(key);	// Search for this key in the BackUp file
			System.out.println(line);
			
			if(line == null) // If not exist
				inserted_list.add(val);	// There is a new entry
			else if(!line.equals(val)) 	// Key Exists so check the 
				updated_list.add(val);
		}
		
		// For every entry of the backup file
	  	for (Map.Entry<String,String> entry : mapBack.entrySet()) {
	  		String key = entry.getKey();
		  	String val = entry.getValue();
		  	String line = mapCur.get(key);
		  	if(line == null) 
		  		deleted_list.add(val);
		}
    	
		br_file.close();
		br_back.close();
		
		// Get line from checksum
		FileInputStream fstream_back2 = new FileInputStream(file_back);
		BufferedReader br_fileback2 = new BufferedReader(new InputStreamReader(fstream_back2));
		
		if(has_header.equals("1"))
			br_fileback2.readLine();	// skip header
		
		String line = br_fileback2.readLine();
		
		while (line != null){
			
			byte[] bytesOfMessage = line.getBytes("UTF-8");
			byte[] thedigest = md.digest(bytesOfMessage);
			
			// convert byte array to Hex String
			StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < thedigest.length; i++)
	        	sb.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16).substring(1));
	        
	        // StringBuffer to String
	        String line_checksum = sb.toString();
			
	        Integer d_offset = deleted_list.indexOf(line_checksum);
	        
	        if (d_offset != -1)
	        	deleted_list.set(d_offset, line);
	        
			line = br_fileback2.readLine();	// read next line
		}
		
		br_fileback2.close();

		FileInputStream fstream2 = new FileInputStream(file);
		BufferedReader br_file2 = new BufferedReader(new InputStreamReader(fstream2));
		
		if(has_header.equals("1"))
			br_file2.readLine();	// skip header
		
		line = br_file2.readLine();
		
		while (line != null){
			
			byte[] bytesOfMessage = line.getBytes("UTF-8");
			byte[] thedigest = md.digest(bytesOfMessage);
			
			// convert byte array to Hex String
			StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < thedigest.length; i++)
	        	sb.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16).substring(1));
	        
	        // StringBuffer to String
	        String line_checksum = sb.toString();
			
	        Integer i_offset = inserted_list.indexOf(line_checksum);
	        Integer u_offset = updated_list.indexOf(line_checksum);
	        
	        if (i_offset != -1)
	        	inserted_list.set(i_offset, line);
	        if (u_offset != -1)
	        	updated_list.set(u_offset, line);
	        
			line = br_file2.readLine();	// read next line
		}
		
		br_file2.close();
		
		PrintWriter out = new PrintWriter(deltaFile);
		for (String temp : inserted_list) out.println("I:"+ temp);
		for (String temp : updated_list) out.println("U:"+ temp);
		for (String temp : deleted_list) out.println("D:"+ temp);
	
		out.close();
		try {
			Process p = Runtime.getRuntime().exec("cp -f " + file + " " + file_back);
			p.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        return inserted_list.size()>0 || updated_list.size()>0 || deleted_list.size()>0;
	}
}