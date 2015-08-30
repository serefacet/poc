package com.serefacet.spark_streaming_poc;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Hello world in Apache Spark
 *
 */
public class SparkTest 
{
    public static void main( String[] args )
    {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf,Durations.seconds(10));
		
		JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);
		
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String t) throws Exception {
				 return Arrays.asList(t.split("\t"));
			}
			
		});
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String,String,Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairDStream<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		wordCounts.print();
		
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		
		
		
		
    }
}
