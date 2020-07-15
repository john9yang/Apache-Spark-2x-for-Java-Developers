package com.packt.sfjd.ch8;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DfExample {
	public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\Users\\john_yang\\DevTools\\Hadoop");
		 SparkConf conf =new SparkConf().setMaster("local").setAppName("Sql");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Employee> empRDD = jsc.parallelize(Arrays.asList(new Employee(1,"Foo"),new Employee(2,"Bar")));
		SQLContext sqlContext = new SQLContext(jsc);

        Dataset<Row> dataSet = sqlContext.createDataFrame(empRDD, Employee.class);

        Dataset<Row> filter = dataSet.filter("empId >1");

		filter.show();

	}
}