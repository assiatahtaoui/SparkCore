package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Ventes {
    public static void main(String[] args) {
        // creation la configuration et sparkContext
        SparkConf conf = new SparkConf().setAppName("Ventes").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // lire le fichier des ventes
        JavaRDD<String> lignes=sc.textFile("ventes.txt");

        // 3. Mapper chaque ligne vers un tuple (ville, prix)
        JavaPairRDD<String, Float> villePrix = lignes.mapToPair(ligne -> {
            String[] parties = ligne.split(" ");
            String ville = parties[1];
            float prix = Float.parseFloat(parties[3]);
            return new Tuple2<>(ville, prix);
        });
        // 4. Réduire par clé (ville) pour obtenir le total

        JavaPairRDD<String, Float> totalParVille = villePrix.reduceByKey((a, b) -> a + b);

        // 5. Afficher les résultats

        totalParVille.collect().forEach(tuple ->
                System.out.println(tuple._1() + " : " + tuple._2())
        );

        // 6. Fermer Spark
        sc.close();


    }
}