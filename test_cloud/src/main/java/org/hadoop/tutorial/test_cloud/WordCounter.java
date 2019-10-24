package org.hadoop.tutorial.test_cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Arrays;

public class WordCounter {
	
	public static void wordCount() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource("/etc/hadoop/conf/core-site.xml");
		conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
		
		FileSystem gcsFs = new Path("gs://ihg-admin-test/airflow/fairscheduler.xml").getFileSystem(conf);
		String path = gcsFs.toString();
		
	SparkConf sparkconf = new SparkConf().setMaster("local").setAppName("Sandeep Word Count");	
	
	JavaSparkContext sparkContext = new JavaSparkContext(sparkconf);
	
	JavaRDD<String> inputfile = sparkContext.textFile("gs://ihg-admin-test/airflow/content_example");
	System.out.println("Input file is taken successfully");
	JavaRDD<String> wordsFromFile = inputfile.flatMap(content -> Arrays.asList(content.split(" ")));
	JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x,y) -> (int) x + (int) y);
	System.out.println("count the content successfully");
	
	countData.saveAsTextFile("CountData");
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			WordCounter.wordCount();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
