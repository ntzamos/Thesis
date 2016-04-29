package com.incrementalupdates.app;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.SparkConf;
import java.io.Serializable;
import java.io.PrintWriter;
import java.io.File;
import java.io.IOException;
import java.io.*;
import java.lang.*;
import java.util.*;
import scala.Tuple2;

public class App
{
	public static void main( String[] args ) throws IOException
	{
		SparkConf conf = new SparkConf().setAppName("HelloTEST").setMaster("mesos://node2:5050");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Map <String, Integer> col_offset = new HashMap<String, Integer>();	// maps column names to indexes
		List <String> header_lst = new ArrayList<String>();

		// Which file to update
		String filename = args[0];

		// File has header
		String has_header = args[1];

		// Unique keys
		String [] unique_keys = args[2].split(",");
		for (int i = 0; i < unique_keys.length; i++)	// Trim spaces
		    unique_keys[i] = unique_keys[i].trim();

		int unique_keys_size = unique_keys.length;

		// Delimeter to split the file
		String delimeter = args[3].trim();

		JavaRDD<String> deltaFile = sc.textFile("/hdfs/updates.csv");		//Read DELTA file from HDFS (Tha to xei valei o socket server)
		JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/" + filename);	//Read file-to-update from HDFS
		JavaRDD<String> header_rdd = null;

		String header = "no header";

		if (has_header.equals("1")){		// if file has header

			header = fileToUpdate.first();	// get Header
			header_lst.add(header);
			header_rdd = sc.parallelize(header_lst);	// Keap header rdd to add it back later

			final String header2 = header;

			fileToUpdate = fileToUpdate.filter(s -> !s.equals(header2));	// Remove header of file rdd

			String [] header_cols = header.split(",");

			// Map column names to indexes
			for (int i = 0; i < header_cols.length; i ++){
				if (Arrays.asList(unique_keys).contains(header_cols[i]))
					col_offset.put(header_cols[i],new Integer(i));
			}

		}

		for (Map.Entry<String,Integer> entry : col_offset.entrySet()) {
		  String key = entry.getKey();
		  Integer value = entry.getValue();
		  System.out.println("col: " + key + ", index: " + value.toString());
	  	}
		System.out.println("======== HEADER ========");
		System.out.println(header);
		System.out.println("======== HEADER ========");
		//=================================================================
		// Filter and get all the INSERTS (I: ..)
		//=================================================================
		JavaRDD<String> inserts_fake = deltaFile.filter(new Function<String, Boolean>() {
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
		// Filter and get all the DELETES (D: ..)
		//=================================================================
		JavaRDD<String> deletes_fake = deltaFile.filter(new Function<String, Boolean>() {
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
		// Inserts: Map unique key to rest of line
		//=================================================================
		JavaPairRDD<String, String> inserts = inserts_fake.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(delimeter);
					String key = "";

					for (int i = 0; i < unique_keys_size; i ++){
						int index;

						if (has_header.equals("1"))	// if there is a header get index from mapping
							index = col_offset.get(unique_keys[i]);
						else						// else just get the index directly
							index = Integer.parseInt(unique_keys[i]);

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
		JavaPairRDD<String, String> deletes = deletes_fake.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(delimeter);
					String key = "";

					for (int i = 0; i < unique_keys_size; i ++){
						int index;

						if (has_header.equals("1"))	// if there is a header get index from mapping
							index = col_offset.get(unique_keys[i]);
						else						// else just get the index directly
							index = Integer.parseInt(unique_keys[i]);

						if (i == unique_keys_size - 1)
							key += str[index];
						else
							key += str[index] + ",";
					}
					return new Tuple2(key, line);
				}
			});

		// printJavaPairRDD(inserts);
		// printJavaPairRDD(deletes);

		JavaPairRDD<String,String> real_inserts = inserts.subtractByKey(deletes);
		JavaPairRDD<String,String> real_updates = inserts.subtractByKey(real_inserts);
		JavaPairRDD<String,String> real_deletes = deletes.subtractByKey(real_updates);

		printJavaPairRDD(real_inserts);
		printJavaPairRDD(real_updates);
		printJavaPairRDD(real_deletes);

		JavaPairRDD<String, String> real_updates_plus = real_updates.mapToPair(
			new PairFunction<Tuple2<String, String>, String, String>() {
				public Tuple2<String, String> call(final Tuple2<String, String> t) {
					String key = "0:" + t._1 ;

					return new Tuple2(key, t._2);
				}
			});

		JavaPairRDD<String, String> real_deletes_plus = real_deletes.mapToPair(
			new PairFunction<Tuple2<String, String>, String, String>() {
				public Tuple2<String, String> call(final Tuple2<String, String> t) {
					String key = "0:" + t._1 ;

					return new Tuple2(key, t._2);
				}
			});
		printJavaPairRDD(real_updates_plus);
		printJavaPairRDD(real_deletes_plus);
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

						if (has_header.equals("1"))	// if there is a header get index from mapping
							index = col_offset.get(unique_keys[i]);
						else						// else just get the index directly
							index = Integer.parseInt(unique_keys[i]);

						if (i == unique_keys_size - 1)
							key += str[index];
						else
							key += str[index] + ",";

					}
					return new Tuple2(key, line);
				}
			});

		System.out.println("File to Update MAPPING");
		printJavaPairRDD(fileToUpdate_pair);

		//=================================================================
		// Remove OLD unique key from fileToUpdate
		//=================================================================
		fileToUpdate_pair = fileToUpdate_pair.subtractByKey(real_updates_plus);
		fileToUpdate_pair = fileToUpdate_pair.subtractByKey(real_deletes_plus);

		JavaRDD<String> real_inserts_rdd = real_inserts.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._2;
			}
		});

		JavaRDD<String> real_updates_rdd = real_updates_plus.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._2;
			}
		});

		JavaRDD<String> real_deletes_rdd = real_deletes.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._2;
			}
		});

		fileToUpdate = fileToUpdate_pair.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._2;
			}
		});

		//=================================================================
		// Add the NEW unique key line to fileToUpdate - UPDATE Happens here
		//=================================================================
		if (has_header.equals("1"))
			fileToUpdate = header_rdd.union(fileToUpdate);
		fileToUpdate = fileToUpdate.union(real_updates_rdd);
		fileToUpdate = fileToUpdate.union(real_deletes_rdd);
		fileToUpdate = fileToUpdate.union(real_inserts_rdd);

		System.out.println("RESULT FILE");
		printJavaRDD(fileToUpdate);

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
