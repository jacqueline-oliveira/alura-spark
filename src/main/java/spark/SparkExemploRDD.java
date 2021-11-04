package spark;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;


public class SparkExemploRDD {
	public static void main(String[] args) {
		
		 SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb+srv://alura:aluno@cluster0.xvcnx.mongodb.net/aluraflix.conteudo")
			      .config("spark.mongodb.output.uri", "mongodb+srv://alura:aluno@cluster0.xvcnx.mongodb.net/aluraflix.conteudo")
			      .getOrCreate();
		 
		  
		    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		  
		    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		  
		    System.out.println(rdd.count());
		    
		    System.out.println(rdd.first().toJson());
		    jsc.close();
	}

}
