package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    private static final int TILE_SIZE = 1201;

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> gpsFile = ctx.textFile("/user/raw_data/simple-gps-points-120312.txt");

        //Ici le but est de determiner dans quelle tuile est le point
        // Ainsi que de determiner, en pourcentage, le décalage depuis le coin supérieur gauche de la tuile
        // pour trouver le bon pixel plus tard.
        // Le shift n'est pas le meme selon si on est Nord/Sud ou Ouest/Est
        JavaPairRDD<Coord, Value> rdd1 = gpsFile.mapToPair(line -> {
            String[] coords = line.split(",");
            long ns = Long.parseLong(coords[0]);
            long we = Long.parseLong(coords[1]);

            long nsKey = ns / 10000000L; // 10^7
            long weKey = we / 10000000L;

            //Rethink this when equal 0 ....
            if (nsKey < 0) { // if south
                nsKey = 90 + Math.abs(nsKey);
            } else {
                nsKey = 90 - nsKey;
            }

            if (weKey < 0) {  //if west
                weKey = 180 - Math.abs(weKey);
            } else {
                weKey = 180 + weKey;
            }
            //String key = weKey + "-" + nsKey + "-" + 0;
            Coord k = new Coord((int)weKey, (int)nsKey, 0);
            float shiftX = (we - (weKey * 10000000L)) / 100000f;
            float shiftY = (ns - (nsKey * 10000000L)) / 100000f;
            Value v = new Value(shiftX, shiftY);
            return new Tuple2<>(k, v);
        });

        //On calcul ici la position du pixel que prendra ce point
        JavaPairRDD<Coord, Tuple2<Integer, Integer>> rdd2 = rdd1.mapValues(v -> {
            int posX = (int) (v.shiftX * TILE_SIZE);
            int posY = (int) (v.shiftX * TILE_SIZE);

            return new Tuple2<>(posX, posY);
        });

        //Dans l'idée il faut aggréger les points par tuiles
        // Il y a peut etre moyen d'optimiser, voir avec Adrien pour une équivalence fonctionnelle plus adaptée
        // Puis il faut voir comment fonctionne groupByKey, si y'a besoin d'un equals ou d'un compareTo ...
        JavaPairRDD<Coord, Iterable<Tuple2<Integer, Integer>>> rdd3 = rdd2.groupByKey(); // ??

        // On crée la tuile du point pour simplifier le traitement (j'avais la flemme de penser autrement
        // puis ca sera plus simple pour plus tard
        JavaPairRDD<Coord, int[]> rdd4 = rdd3.mapValues(it -> {
            int[] tile = new int[TILE_SIZE * TILE_SIZE];
            Arrays.fill(tile, 0);
            it.forEach(tuple -> {
                int x = tuple._1;
                int y = tuple._2;
                int index = x + y * TILE_SIZE;
                tile[index] = tile[index] + 1;
            });
            return tile;
        });

        // On commence les aggrégations des résultats précédents, il faut peut etre mettre en cache le rdd4 pour l'envoyer en
        // BDD, similaire au groupeByKey, il faut regarder ce qu'il faut quand on utilise des objets customs pour que ca fonctionne
        // equals ? compareTo ?...
        JavaPairRDD<Coord, Iterable<Tuple2<Coord, int[]>>> rdd5Aggr = rdd4.groupBy(t -> {
            Coord coord = t._1;
            int newX = coord.x / 2;
            int newY = coord.y / 2;
            return new Coord(newX, newY, coord.z + 1);
        });


        // Ici le but est de composer la grande tuile
        // Et de recopier chaque mini tuile dans la grande tuile dans le bon coin.
        JavaPairRDD<Coord, int[]> rdd6Aggr = rdd5Aggr.mapValues(it -> {
            int[] newTile = new int[TILE_SIZE * TILE_SIZE * 4];
            Arrays.fill(newTile, 0);
            it.forEach(tuple -> {
                Coord coord = tuple._1;
                int shiftX = TILE_SIZE * (coord.x % 2);
                int shiftY = TILE_SIZE * (coord.y % 2);
                int[] tile = tuple._2;
                for (int y = 0; y < TILE_SIZE; ++y) {
                    for (int x = 0; x < TILE_SIZE; ++x) {
                        int index = (x + shiftX) + (y + shiftY) * TILE_SIZE * 2;
                        int currIndex = x + y * TILE_SIZE;
                        newTile[index] += tile[currIndex];
                    }
                }
            });
            return newTile;
        });

        // On va maintenant aggréger les valeurs de la grande tuile vers la tuile de taille normale.
        // Avoir la somme par pixel va permettre de jouer sur les nuances de gris pour plus tard.
        JavaPairRDD<Coord, int[]> rdd7aggr = rdd6Aggr.mapValues(bigTile -> {
            int[] finalTile = new int[TILE_SIZE * TILE_SIZE];
            for (int y = 0; y < TILE_SIZE; ++y) {
                for (int x = 0; x < TILE_SIZE; ++x) {
                    int sum = 0;
                    for (int j = y*2; j <= y*2 + 1; ++j) {
                        for (int i = x*2; i <= x*2 + 1; ++x) {
                            sum += bigTile[i + j * TILE_SIZE];
                        }
                    }
                    finalTile[x + y * TILE_SIZE] = sum;
                }
            }
            return finalTile;
        });

        // Ici je pense avoir fini le plus gros du traitement, voir avec Adrien, et lancer des tests sur de petits ensemble
        // Quelques doutes à confirmer avec Adrien sur la premiere etape concernant la conversion des points GPS du fichier
        // en position dans un tableau 2D ....
        // Aussi quelques doutes sur les groupeBy ...
        // Il faut aussi revoir le rdd6Aggr sur les shiftX, ShiftY ... pour etre sur que j'ai rien oublié
        // Et faut implémenter le dao etc ...

        // Il faut regenerer les tuiles en corrigeant le 180 NS
    }
}
