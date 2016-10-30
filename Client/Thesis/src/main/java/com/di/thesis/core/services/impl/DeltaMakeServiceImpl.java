package com.di.thesis.core.services.impl;

import com.di.thesis.core.services.DeltaMakerService;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

import java.io.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DeltaMakeServiceImpl implements DeltaMakerService{

    private MessageDigest md;
    private List<String> inserted_list = new ArrayList<>();
    private List<String> updated_list = new ArrayList<>();
    private List<String> deleted_list = new ArrayList<>();

    public DeltaMakeServiceImpl() {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public boolean createDelta(String fileName, String hasHeader, String uniqueKeys, String delimeter) {
        String[] keysArray = uniqueKeys.split(delimeter);
        Integer[] keysIndex = new Integer[keysArray.length];

        try {

            CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                    .withCache("mapCur", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(1000)).build())
                    .withCache("mapBack", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(1000)).build())
                    .build(true);

            Cache<String, String> mapCur = cacheManager.getCache("mapCur", String.class, String.class);
            Cache<String, String> mapBack = cacheManager.getCache("mapBack", String.class, String.class);

            String file_back = fileName + ".back";
            String deltaFile = fileName + ".delta";

            File backup = new File(file_back);

            if(!backup.exists()) {

                FileInputStream fstream = new FileInputStream(fileName);
                BufferedReader br_file = new BufferedReader(new InputStreamReader(fstream));

                PrintWriter out = new PrintWriter(deltaFile);

                String fileLine = br_file.readLine();
                int cnt=0;

                while (fileLine != null) {
                    if(cnt == 0 && hasHeader.equals("1"))
                        out.println(fileLine);
                    else
                        out.println(fileLine + ":0");

                    cnt++;
                    fileLine = br_file.readLine();
                }

                try {
                    Process p = Runtime.getRuntime().exec("cp -f " + fileName + " " + file_back);
                    p.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                out.close();
                br_file.close();
                return true;
            }

            FileInputStream fstream = new FileInputStream(fileName);
            BufferedReader br_file = new BufferedReader(new InputStreamReader(fstream));

            FileInputStream fstreamback = new FileInputStream(file_back);
            BufferedReader br_back = new BufferedReader(new InputStreamReader(fstreamback));

            if(hasHeader.equals("1")) {
                br_back.readLine();
                String header = br_file.readLine();

                String[] headerArray = header.split(delimeter);

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
            String fileLine = br_file.readLine();

            while (fileLine != null) {
                String key = "";
                String[] line = fileLine.split(delimeter);

                for(int i = 0; i < keysIndex.length; i++) {
                    if (i == keysIndex.length - 1)
                        key += line[keysIndex[i]];
                    else
                        key += line[keysIndex[i]] + delimeter;
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
            String backLine = br_back.readLine();

            while (backLine != null) {
                String key = "";
                String[] line = backLine.split(delimeter);

                for(int i = 0; i < keysIndex.length; i++) {
                    if (i == keysIndex.length - 1)
                        key += line[keysIndex[i]];
                    else
                        key += line[keysIndex[i]] + delimeter;
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
            printMap(mapCur);
            System.out.println("PRINTING BACKUP FILE MAP");
            printMap(mapBack);

            Iterator<Cache.Entry<String,String>> it3 = mapCur.iterator();

            while (it3.hasNext()){
                Cache.Entry<String,String> a = it3.next();
                String key = a.getKey();
                String val = a.getValue();
                String line = mapBack.get(key);	// Search for this key in the BackUp file

                if(line == null) // If not exist
                    inserted_list.add(val);	// There is a new entry
                else if(!line.equals(val)) 	// Key Exists so check the
                    updated_list.add(val);
            }

            Iterator<Cache.Entry<String,String>> it4 = mapBack.iterator();

            // For every entry of the backup file
            while (it4.hasNext()){
                Cache.Entry<String,String> a = it4.next();
                String key = a.getKey();
                String val = a.getValue();
                String line = mapCur.get(key);

                if(line == null)
                    deleted_list.add(val);
            }

            br_file.close();
            br_back.close();

            // Get line from checksum
            FileInputStream fstream_back2 = new FileInputStream(file_back);
            BufferedReader br_fileback2 = new BufferedReader(new InputStreamReader(fstream_back2));

            if (hasHeader.equals("1"))
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

            FileInputStream fstream2 = new FileInputStream(fileName);
            BufferedReader br_file2 = new BufferedReader(new InputStreamReader(fstream2));

            if(hasHeader.equals("1"))
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
                Process p = Runtime.getRuntime().exec("cp -f " + fileName + " " + file_back);
                p.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return inserted_list.size() > 0 || updated_list.size() > 0 || deleted_list.size() > 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private void printMap(Cache<String, String> map) {

        Iterator<Cache.Entry<String,String>> it = map.iterator();

        while (it.hasNext()){
            Cache.Entry<String,String> a = it.next();
            String key = a.getKey();
            String val = a.getValue();
            System.out.println("Key: " + key + " Value: " + val);
        }
    }

}
