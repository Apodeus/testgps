package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {


    public static void main(String[] args) throws Exception {
        GPSProgram gpsProgram = new GPSProgram();
        gpsProgram.run();

        // Ici je pense avoir fini le plus gros du traitement, voir avec Adrien, et lancer des tests sur de petits ensemble
        // Quelques doutes à confirmer avec Adrien sur la premiere etape concernant la conversion des points GPS du fichier
        // en position dans un tableau 2D ....
        // Aussi quelques doutes sur les groupeBy ...
        // Il faut aussi revoir le rdd6Aggr sur les shiftX, ShiftY ... pour etre sur que j'ai rien oublié
        // Et faut implémenter le dao etc ...

        // Il faut regenerer les tuiles en corrigeant le 180 NS
    }
}
