package com.iwinner.spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

public class FileAccess {

	
    static String inputpath,masterurl;
	static JavaSparkContext sc;
	static HiveContext sql;
	
		
	public static void init(String master){
		
		SparkConf conf = new SparkConf().setMaster(master).setAppName("test-spark");
		sc = new JavaSparkContext(conf);
		sql=new HiveContext(sc);
		
	}
	
	
		
	public static void main(String...arg)
	{
		StringBuilder sbd = new StringBuilder();
		
		if(arg.length==0){
			
			System.out.println("No arguments set");
			System.out.println("Using default args");
			masterurl="local[*]";
			inputpath="D:\\input.txt";
		}
		else{
			
			masterurl=arg[0];
			inputpath=arg[1];
		}
		
	  	
	   init(masterurl);
	   JavaRDD<String> input = sc.textFile(inputpath);
	   JavaRDD<String> words = input.flatMap(
			      new FlatMapFunction<String, String>() {
			        public Iterable<String> call(String x) {
			          return Arrays.asList(x.split(" "));
			        }});
	   
	   	
			    
			    JavaPairRDD<String, Integer> counts = words.mapToPair(
			      new PairFunction<String, String, Integer>(){
			        public Tuple2<String, Integer> call(String x){
			          return new Tuple2(x, 1);
			        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
			            public Integer call(Integer x, Integer y){ return x + y;}});
	 counts.saveAsTextFile("D:/output.txt");  
	   
	/* while( counts.collect().iterator().hasNext())
	 {
		 
		 System.out.println(counts.collect().iterator().next());
	 }*/
	  //counts.saveAsTextFile("");
	   
	   
	}	
	
}