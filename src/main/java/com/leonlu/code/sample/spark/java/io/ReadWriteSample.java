package com.leonlu.code.sample.spark.java.io;

import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
/**
 * sample of reading and writing in Spark
 * tested in Spark 1.4.1
 * @author leon
 *
 */
public class ReadWriteSample {
	private static SparkConf conf = new SparkConf().setAppName("spark-java-test").setMaster("local");
	
	public static void readFromFileWithWholeTextFiles() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//what the `wholeTextFiles` return is a Pair RDD
		//read the files at a local path just for a test
		String localSampleFilePath = ReadWriteSample.class.getClassLoader().getResource("data").getPath();
		JavaPairRDD<String, String> pairRdd = sc.wholeTextFiles("file://" + localSampleFilePath);
		
		//print every filename and content
		pairRdd.foreach(new VoidFunction<Tuple2<String, String>>(){
			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t._1 + "," + t._2);
				
			}
		});
		
		//convert the Pair RDD to a RDD 
		JavaRDD<String> rdd = pairRdd.map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				return v1._2;
			}
			
		});
		//collect and print the contents
		Iterator<String> a = rdd.collect().iterator();
		while(a.hasNext()) {
			System.out.println(a.next());
		}
		sc.close();
	}
	
	public static void writeToDatabaseWithJdbc() {
		//postgresql
		final String sampleTargetPostgresqlUrl = "jdbc:postgresql://<IP_OR_HOSTNAME>:5432/wikidemo";
		final String sampleTargetTable = "people";
		final String sampleTargetPostgresqlUsername = "<USERNAME>";
		final String sampleTargetPostgresqlPwd = "<PASSWORD>";
		final String sampleTargetPostgresqlDriver = "org.postgresql.Driver";	
		
		Properties targetPostgresqlConnProperties = new Properties();
		targetPostgresqlConnProperties.put("user",  sampleTargetPostgresqlUsername);
		targetPostgresqlConnProperties.put("password", sampleTargetPostgresqlPwd);
		targetPostgresqlConnProperties.put("driver", sampleTargetPostgresqlDriver);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		//read local file in Json format
		final String sampleSourceUrl = "file://" + 
				ReadWriteSample.class.getClassLoader().getResource("data/people-1.txt").getPath();
		DataFrame sampleDF = sqlContext.read().json(sampleSourceUrl);
		
		//write to database, it will create a new table if the table does not exist
		final String dataframeSaveMode = "append";
		sampleDF.write().mode(dataframeSaveMode).jdbc(sampleTargetPostgresqlUrl, sampleTargetTable,
				targetPostgresqlConnProperties);
		
		//read and show
		sqlContext.read()
			.jdbc(sampleTargetPostgresqlUrl, sampleTargetTable, targetPostgresqlConnProperties)
			.show();
		
		sc.close();
	}
	public static void main(String args[]) {
		//readFromFileWithWholeTextFiles();
		//writeToDatabaseWithJdbc();
	}
}
