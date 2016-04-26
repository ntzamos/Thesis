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

		//=================================================================
		// Read DELTA file from hdfs (Tha to xei valei o socket server)
		//=================================================================
		JavaRDD<String> deltaFile = sc.textFile("/hdfs/updates.csv");

		//=================================================================
		// Filter and get all the inserts (I: ..)
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

		List<String> mylist = inserts.collect();

		for (String i : mylist){
			System.out.println(i);
		}
		//==============================
		// Get file-to-update from HDFS
		//==============================
		// JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/" + fileToUpdate);
		JavaRDD<String> fileToUpdate = sc.textFile("/hdfs/file1.csv");	// TEST - USE THE ABOVE LATER
		JavaRDD<String> result = fileToUpdate.union(inserts);

		List<String> ls = result.collect();

		for (String l: ls){
			System.out.println(l);
		}

		System.out.println( "Hello World22!" );
	}
}
