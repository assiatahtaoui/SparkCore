package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class VentesParVille {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("venteVille").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lignes = sc.textFile("venteVille.txt");

        // En-tête
        String header = lignes.first();
        JavaRDD<String> data = lignes.filter(row -> !row.equals(header));

        JavaPairRDD<Tuple2<String, String>, Double> villeAnneePrix = data.mapToPair(row -> {
            String[] champs = row.split(",");
            String ville = champs[0];
            String date = champs[1];
            int quantite = Integer.parseInt(champs[3]);
            double prixUnitaire = Double.parseDouble(champs[4]);

            String annee = date.split("-")[0];
            double prixTotal = quantite * prixUnitaire;

            return new Tuple2<>(new Tuple2<>(ville, annee), prixTotal);
        });

        JavaPairRDD<Tuple2<String, String>, Double> totalParVilleAnnee = villeAnneePrix.reduceByKey(Double::sum);

        totalParVilleAnnee.foreach(resultat -> {
            System.out.println("Ville: " + resultat._1._1 + ", Année: " + resultat._1._2 + ", Total: " + resultat._2);
        });

        sc.close();
    }
}
