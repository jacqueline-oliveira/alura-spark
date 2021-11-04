package spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

public class SparkExemploDataset {

	public static void main(String[] args) {
		 SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb+srv://alura:aluno@cluster0.xvcnx.mongodb.net/aluraflix.conteudo")
			      .config("spark.mongodb.output.uri", "mongodb+srv://alura:aluno@cluster0.xvcnx.mongodb.net/aluraflix.conteudo")
			      .getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		Dataset<Row> conteudo = MongoSpark.load(jsc).toDF();
		conteudo.printSchema();
		conteudo.show();

		conteudo.createOrReplaceTempView("conteudo");

		Dataset<Row> consulta = spark.sql("SELECT titulo FROM conteudo WHERE dataPublicacao >= '2020-01-30' ");
		consulta.show();
		
		jsc.close();

	}

}
