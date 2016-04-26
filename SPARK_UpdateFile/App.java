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

		// Which file to update
		String filename = args[0];

		// SizeOf unique key
		int key_size = Integer.parseInt(args[1]);

		//=================================================================
		// Read DELTA file from HDFS (Tha to xei valei o socket server)
		//=================================================================
		JavaRDD<String> deltaFile = sc.textFile("/hdfs/updates.csv");
		//==================================
		// Read file-to-update from HDFS
		//==================================
		// JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/" + fileToUpdate);
		JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/file1.csv");	// TEST - USE THE ABOVE LATER

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
		// Filter and get all the UPDATES (I: ..)
		//=================================================================
		JavaRDD<String> updates = deltaFile.filter(new Function<String, Boolean>() {
			public Boolean call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				String operation = parts[0];			// Operation can be 'I', 'D' or 'U'
				return (operation.equals("U"));			// if Update operation keep this

			}
		}).map(new Function<String, String>() {			// Map in order to get after I: line
			public String call(String strLine) {
				String [] parts = strLine.split(":");	// I: x,y,z format
				return parts[1]+":0";					// right part of line
			}
		});
		//=================================================================
		// fileToUpdate: Map unique key to rest of line
		//=================================================================
		JavaPairRDD<String, String> pairRdd = fileToUpdate.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(",", key_size + 1);
					String key = "";
					for (int i = 0; i < str.length - 1; i++){
						if (i == str.length - 2)
							key += str[i];
						else
							key += str[i] + ",";
					}
					return new Tuple2(key, str[str.length - 1]);
				}
			});

		//=================================================================
		// DELTA: Map unique key to rest of line
		//=================================================================
		JavaPairRDD<String, String> pairRdd2 = updates.mapToPair(
			new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(final String line) {
					String[] str = line.split(",", key_size + 1);
					String key = "";
					for (int i = 0; i < str.length - 1; i++){
						if (i == str.length - 2)
							key += str[i];
						else
							key += str[i] + ",";
					}
					return new Tuple2(key, str[str.length - 1]);
				}
			});
		//=================================================================
		// Remove OLD unique key from fileToUpdate
		//=================================================================
		JavaPairRDD<String, String> pairRdd3 = pairRdd.subtractByKey(pairRdd2);

		//=================================================================
		// Add the NEW unique key line to fileToUpdate - UPDATE Happens here
		//=================================================================
		pairRdd3 = pairRdd3.union(pairRdd2);

		printJavaPairRDD(pairRdd);

		JavaRDD<String> newrdd = pairRdd.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._1 + "," + t._2;
			}
		});

		JavaRDD<String> newrdd2 = pairRdd2.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._1 + "," + t._2;
			}
		});

		printJavaRDD(newrdd);
		printJavaRDD(newrdd2);


		JavaRDD<String> newrdd3 = pairRdd3.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> t) {
				return t._1 + "," + t._2;
			}
		});

		printJavaRDD(newrdd3);

		// ADD INSERTS
		JavaRDD<String> result = newrdd3.union(inserts);

		System.out.println("RESULT FILE");
		printJavaRDD(result);

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
