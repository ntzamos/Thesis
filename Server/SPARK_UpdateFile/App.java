package com.incrementalupdates.app;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.SparkConf;
import org.apache.hadoop.conf.Configuration;
import java.io.Serializable;
import java.io.PrintWriter;
import java.io.File;
import java.io.IOException;
import java.io.*;
import java.lang.*;
import java.util.*;
import scala.Tuple2;
import org.apache.hadoop.fs.*;

public class App
{
	public static void main( String[] args ) throws IOException
	{
		SparkConf conf = new SparkConf().setAppName("HelloTEST").setMaster("mesos://node2:5050");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Map <String, Integer> col_offset = new HashMap<String, Integer>();	// Maps column names to indexes
		List <String> header_lst = new ArrayList<String>();

		// Which file to update
		String filename = args[0].trim();

		// File has header
		String has_header = args[1];

		// Unique keys
		String [] unique_keys = args[2].split(",");
		for (int i = 0; i < unique_keys.length; i++)	// Trim spaces
		    unique_keys[i] = unique_keys[i].trim();

		int unique_keys_size = unique_keys.length;

		// Delimeter to split the file
		String delimeter = args[3].trim();

		JavaRDD<String> deltaFile = sc.textFile("/hdfs/delta");				//Read DELTA file from HDFS (Tha to xei valei o socket server)
		JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/" + filename);	//Read file-to-update from HDFS
		JavaRDD<String> header_rdd = null;

		String header = "no header";

		if (has_header.equals("1")){	// If file has header

			// Get Header
			header = fileToUpdate.first();

			// Keap header rdd to add it back later
			header_lst.add(header);
			header_rdd = sc.parallelize(header_lst);

			final String header2 = header;

			// Remove header of file rdd
			fileToUpdate = fileToUpdate.filter(s -> !s.equals(header2));

			String [] header_cols = header.split(",");

			// Map column names to indexes
			for (int i = 0; i < header_cols.length; i ++){
				if (Arrays.asList(unique_keys).contains(header_cols[i]))
					col_offset.put(header_cols[i],new Integer(i));
			}

		}

		// for (Map.Entry<String,Integer> entry : col_offset.entrySet()) {
		//   String key = entry.getKey();
		//   Integer value = entry.getValue();
		//   System.out.println("col: " + key + ", index: " + value.toString());
		//  	}
		// System.out.println("=========== HEADER ===========");
		// System.out.println(header);
		//=================================================================
		// Filter and get all the INSERTS (I: ..)
		//=================================================================
		JavaRDD<String> inserts = deltaFile.filter(new Function<String, Boolean>() {
			public Boolean call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				String operation = parts[0];			// Operation can be 'I', 'D' or 'U'
				return (operation.equals("I"));			// if Insert operation keep this
			}

		}).map(new Function<String, String>() {			// Map in order to get after I: line
			public String call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				return parts[1]+":0";					// right part of line
			}
		});
		//=================================================================
		// Filter and get all the UPDATES (U: ..)
		//=================================================================
		JavaRDD<String> updates = deltaFile.filter(new Function<String, Boolean>() {
			public Boolean call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				String operation = parts[0];			// Operation can be 'I', 'D' or 'U'
				return (operation.equals("U"));			// if Insert operation keep this
			}
		}).map(new Function<String, String>() {			// Map in order to get after I: line
			public String call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				return parts[1]+":0";					// right part of line
			}
		});
		//=================================================================
		// Filter and get all the DELETES (D: ..)
		//=================================================================
		JavaRDD<String> deletes = deltaFile.filter(new Function<String, Boolean>() {
			public Boolean call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				String operation = parts[0];			// Operation can be 'I', 'D' or 'U'
				return (operation.equals("D"));			// if Update operation keep this

			}
		}).map(new Function<String, String>() {			// Map in order to get after I: line
			public String call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				return parts[1]+":1";					// right part of line
			}
		});

		//=================================================================
		// Updates: Map unique key to rest of line
		//=================================================================
		JavaPairRDD<String, String> updates_pair = updates.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(delimeter);
					String key = "0:";

					for (int i = 0; i < unique_keys_size; i ++){
						int index;
						String key_to_add = unique_keys[i];

						if (has_header.equals("1"))	// if there is a header get index from mapping
							index = col_offset.get(key_to_add);
						else						// else just get the index directly
							index = Integer.parseInt(key_to_add);

						// Last column case
						if (str[index].contains(":0") || str[index].contains(":1"))
							str[index] = str[index].split(":")[0];

						if (i == unique_keys_size - 1)
							key += str[index];
						else
							key += str[index] + ",";
					}
					return new Tuple2(key, line);
				}
			});
		//=================================================================
		// Deletes: Map unique key to rest of line
		//=================================================================
		JavaPairRDD<String, String> deletes_pair = deletes.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(delimeter);
					String key = "0:";

					for (int i = 0; i < unique_keys_size; i ++){
						int index;
						String key_to_add = unique_keys[i];

						if (has_header.equals("1"))	// if there is a header get index from mapping
							index = col_offset.get(key_to_add);
						else						// else just get the index directly
							index = Integer.parseInt(key_to_add);

						// Last column case
						if (str[index].contains(":0") || str[index].contains(":1"))
							str[index] = str[index].split(":")[0];

						if (i == unique_keys_size - 1)
							key += str[index];
						else
							key += str[index] + ",";
					}
					return new Tuple2(key, line);
				}
			});
		// System.out.println("================= INSERTS ==================");
		// printJavaRDD(inserts);
		// System.out.println("================= UPDATES==================");
		// printJavaPairRDD(updates_pair);
		// System.out.println("================= DELETES ==================");
		// printJavaPairRDD(deletes_pair);
		// System.out.println("=================================================================");

		//=================================================================
		// fileToUpdate: Map unique key to rest of line
		//=================================================================
		System.out.println("---------------------------------------------------");

		JavaPairRDD<String, String> fileToUpdate_pair = fileToUpdate.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(delimeter);
					String key = Character.toString(line.charAt(line.length() - 1)) + ":";

					for (int i = 0; i < unique_keys_size; i ++){
						int index;
						String key_to_add = unique_keys[i];

						if (has_header.equals("1"))	// if there is a header get index from mapping
							index = col_offset.get(key_to_add);
						else						// else just get the index directly
							index = Integer.parseInt(key_to_add);

						// Last column case
						if (str[index].contains(":0") || str[index].contains(":1"))
							str[index] = str[index].split(":")[0];

						if (i == unique_keys_size - 1)
							key += str[index];
						else
							key += str[index] + ",";

					}
					return new Tuple2(key, line);
				}
			});

		// System.out.println("File to Update MAPPING");
		// printJavaPairRDD(fileToUpdate_pair);

		// Remove OLD update lines from fileToUpdate
		fileToUpdate_pair = fileToUpdate_pair.subtractByKey(updates_pair);
		// Remove OLD delete lines from fileToUpdate
		fileToUpdate_pair = fileToUpdate_pair.subtractByKey(deletes_pair);

		// Convert file to update pair back to rdd in order to union it
		fileToUpdate = fileToUpdate_pair.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t){
				return t._2; }
		});

		//===================================================================
		// Add the NEW unique key line to fileToUpdate - UPDATE Happens here
		//===================================================================
		if (has_header.equals("1"))
			fileToUpdate = header_rdd.union(fileToUpdate);
		fileToUpdate = fileToUpdate.union(updates);
		fileToUpdate = fileToUpdate.union(deletes);
		fileToUpdate = fileToUpdate.union(inserts);

		// System.out.println("RESULT FILE");
		// printJavaRDD(fileToUpdate);

		// Kinda overwrite xD
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path newFolderPath = new Path("/hdfs/" +filename);

		if(hdfs.exists(newFolderPath)){
			System.out.println("RE GAMW");
			hdfs.delete(newFolderPath, true); //Delete existing Directory
		}

		fileToUpdate.coalesce(1).saveAsTextFile("/hdfs/" + filename);

	}

	public static void printJavaRDD(JavaRDD<String> rdd){
		List<String> rddlist = rdd.collect();

		for (String i : rddlist){
			System.out.println(i);
		}
		System.out.println("-------------------------------------------------");
	}

	public static void printJavaPairRDD(JavaPairRDD<String, String> pairRdd){
		Map<String,String> mymap = pairRdd.collectAsMap();

		for (Map.Entry<String,String> entry : mymap.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			System.out.println("(("+key+")," + value + ")");
		}
		System.out.println("-------------------------------------------------");
	}
}
