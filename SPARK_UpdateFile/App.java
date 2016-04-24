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

		List <String> inserts = new ArrayList<String>();

		//========================
		// Read local DELTA file
		//========================
		FileInputStream fstream = new FileInputStream("/home/cluster/ptyxiaki/updates.csv");
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;

		while ((strLine = br.readLine()) != null)   {

			// I: x,y,z format
			String [] parts = strLine.split(":");

			// Operation can be 'I', 'D' or 'U'
			String operation = parts[0];

			// Line to be inserted
			String line_to_add = parts[1];

			if (operation.equals("I"))
				inserts.add(line_to_add);
		}

		//Close input stream
		br.close();

		// Convert List to Rdd
		JavaRDD<String> insert_lines = sc.parallelize(inserts);

		//==============================
		// Get file-to-update from HDFS
		//==============================
		// JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/" + fileToUpdate);
		JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/file1.csv");	// TEST

		JavaRDD<String> result = fileToUpdate.union(insert_lines);

		List<String> ls = result.collect();

		for (String l: ls){
			System.out.println(l);
		}

		System.out.println( "Hello World22!" );
    }
}
