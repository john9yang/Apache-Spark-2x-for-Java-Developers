package com.packt.sfjd.ch8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionHeloWorld {
public static void main(String[] args) {
	System.setProperty("hadoop.home.dir", "C:\\Users\\john_yang\\DevTools\\Hadoop");
	SparkSession sparkSession = SparkSession.builder()
			.master("local")
			.appName("CSV Read Example")
			.config("spark.sql.warehouse.dir", "file:////C:/Users/john_yang/spark-warehouse")
			.getOrCreate();
	
	Dataset<Row> csv = sparkSession.read().format("com.databricks.spark.csv").option("header","true")
			.load("C:\\Users\\john_yang\\Study\\Code\\Apache-Spark-2x-for-Java-Developers\\src\\main\\resources\\emp.csv");

	csv.show();
	
	csv.createOrReplaceTempView("test");
	Dataset<Row> sql = sparkSession.sql("select * from test");
	System.out.println(sql.collectAsList());
}
}
