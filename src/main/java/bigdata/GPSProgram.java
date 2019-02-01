package bigdata;

import org.apache.commons.math.geometry.Vector3D;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SortedList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class GPSProgram {

    public static final String STORAGE_HBASE_GPS = "storage/hbaseGPS_";
    public static final byte[] TILES = Bytes.toBytes("tile");
    public static final byte[] GPS = Bytes.toBytes("gps");
    private final JavaSparkContext ctx;
    private static final int TILE_SIZE = 1201;
    private final CompressionManager compressionManager;
    private final HBaseDAO hbaseDAO;

    public GPSProgram(){
        SparkConf sparkConf = new SparkConf();
        ctx = new JavaSparkContext(sparkConf);
        compressionManager = new CompressionManager();
        hbaseDAO = new HBaseDAO();
    }


    public void run() throws Exception {

        JavaRDD<String> gpsFile = ctx.textFile("/user/raw_data/simple-gps-points-120312.txt");

        JavaRDD<String> rddDistinct = gpsFile.map(line -> {
            String[] tok = line.split(",");
            long lat = Long.parseLong(tok[0]);
            long longitude = Long.parseLong(tok[1]);

            long lat2 = lat / 1000;
            long long2 = longitude / 1000;

            return lat2 + "," + long2;
        }).distinct();

        //Ici le but est de determiner dans quelle tuile est le point
        // Ainsi que de determiner, en pourcentage, le décalage depuis le coin supérieur gauche de la tuile
        // pour trouver le bon pixel plus tard.
        // Le shift n'est pas le meme selon si on est Nord/Sud ou Ouest/Est
        JavaPairRDD<String, Value> rdd1 = rddDistinct.mapToPair(line -> {
            String[] coords = line.split(",");
            long ns = Long.parseLong(coords[0]);
            long we = Long.parseLong(coords[1]);

            long nsKey = ns / 10000L; // 10^7
            long weKey = we / 10000L;

            //Rethink this when equal 0 ....
            float shiftX;
            float shiftY;
            if (ns <= 0) { // if south
                nsKey = 91 + Math.abs(nsKey);
                shiftY = ((ns / 10000f) % 1);
            } else {
                nsKey = 91 - (nsKey + 1);
                shiftY = 1 - ((Math.abs(ns) / 10000f) % 1);
            }

            if (we < 0) {  //if west
                weKey = Math.max(0, 180 - (Math.abs(weKey) + 1));
                shiftX = 1 - ((Math.abs(we) / 10000f) % 1);
            } else {
                weKey = 180 + weKey;
                shiftX = (we / 10000f) % 1;
            }
            String key = (int)weKey + "-" + (int)nsKey + "-" + 0;
            Value v = new Value(Math.abs(shiftX), Math.abs(shiftY)); // pourcentage entre 0 et 1
            return new Tuple2<>(key, v);
        });

        //On calcul ici la position du pixel que prendra ce point
        JavaPairRDD<String, Tuple2<Integer, Integer>> rdd2 = rdd1.mapValues(v -> {
            int posX = (int) (v.shiftX * (TILE_SIZE - 1));
            int posY = (int) (v.shiftY * (TILE_SIZE - 1));
            return new Tuple2<>(posX, posY);
        });

        //Dans l'idée il faut aggréger les points par tuiles
        // Il y a peut etre moyen d'optimiser, voir avec Adrien pour une équivalence fonctionnelle plus adaptée
        // Puis il faut voir comment fonctionne groupByKey, si y'a besoin d'un equals ou d'un compareTo ...
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> rdd3 = rdd2.groupByKey(); // ??

        // On crée la tuile du point pour simplifier le traitement (j'avais la flemme de penser autrement
        // puis ca sera plus simple pour plus tard
        JavaPairRDD<String, boolean[]> rdd4 = rdd3.mapValues(it -> {
            boolean[] tile = new boolean[TILE_SIZE * TILE_SIZE];
            Arrays.fill(tile, false);
            it.forEach(tuple -> {
                int x = tuple._1;
                int y = tuple._2;
                int index = x + y * TILE_SIZE;
                tile[index] = true;
            });
            return tile;
        }).cache();

        JavaPairRDD<String, byte[]> rdd5 = rdd4.mapValues(tile -> {
            int length = tile.length;
            ByteBuffer bb = ByteBuffer.allocate(length *  Byte.BYTES);
            for (int i = 0; i < length; ++i) {
                bb.put(tile[i] ? (byte)1 : (byte) 0);
            }
            return bb.array();
        });

        JavaPairRDD<String, byte[]> compressRdd5 = compressionManager.compress(rdd5);

        hbaseDAO.bulkSaveRDD(compressRdd5, STORAGE_HBASE_GPS + "0", TILES, GPS);

        int aggr = 8;
        for(int nbAggr = 0; nbAggr < aggr; ++nbAggr){

            // On commence les aggrégations des résultats précédents, il faut peut etre mettre en cache le rdd4 pour l'envoyer en
            // BDD, similaire au groupeByKey, il faut regarder ce qu'il faut quand on utilise des objets customs pour que ca fonctionne
            // equals ? compareTo ?...
            JavaPairRDD<String, Iterable<Tuple2<String, boolean[]>>> rdd5Aggr = rdd4.groupBy(t -> {
                String[] coords = t._1.split("-");
                int newX = Integer.parseInt(coords[0]) / 2;
                int newY = Integer.parseInt(coords[1]) / 2;
                return newX + "-" + newY + "-" + (Integer.parseInt(coords[2]) + 1);
            });

            // Ici le but est de composer la grande tuile
            // Et de recopier chaque mini tuile dans la grande tuile dans le bon coin.
            JavaPairRDD<String, boolean[]> rdd6Aggr = rdd5Aggr.mapValues(it -> {
                boolean[] bigTile = new boolean[TILE_SIZE * TILE_SIZE * 4];
                Arrays.fill(bigTile, false);
                it.forEach(tuple -> {
                    String[] coords = tuple._1.split("-");
                    int coordX = Integer.parseInt(coords[0]);
                    int coordY = Integer.parseInt(coords[1]);

                    int shiftX = TILE_SIZE * (coordX % 2);
                    int shiftY = TILE_SIZE * (coordY % 2);

                    boolean[] tile = tuple._2;
                    for (int y = 0; y < TILE_SIZE; ++y) {
                        for (int x = 0; x < TILE_SIZE; ++x) {
                            int indexBigTile = (x + shiftX) + ((y + shiftY) * TILE_SIZE * 2);
                            int currIndex = x + y * TILE_SIZE;
                            bigTile[indexBigTile] = tile[currIndex];
                        }
                    }
                });
                return bigTile;
            });

            // On va maintenant aggréger les valeurs de la grande tuile vers la tuile de taille normale.
            // Avoir la somme par pixel va permettre de jouer sur les nuances de gris pour plus tard.
            JavaPairRDD<String, boolean[]> rdd7aggr = rdd6Aggr.mapValues(bigTile -> {
                boolean[] finalTile = new boolean[TILE_SIZE * TILE_SIZE];
                for (int y = 0; y < TILE_SIZE; ++y) {
                    for (int x = 0; x < TILE_SIZE; ++x) {
                        boolean isPresent = false;
                        for (int j = y*2; j <= y*2 + 1; ++j) {
                            for (int i = x*2; i <= x*2 + 1; ++i) {
                                isPresent = isPresent || bigTile[i + j * TILE_SIZE * 2];
                            }
                        }
                        finalTile[x + y * TILE_SIZE] = isPresent;
                    }
                }
                return finalTile;
            }).persist(StorageLevel.DISK_ONLY());

            JavaPairRDD<String, byte[]> rdd8 = rdd7aggr.mapValues(tile -> {
                int length = tile.length;
                ByteBuffer bb = ByteBuffer.allocate(length * Byte.BYTES);
                for (int ind = 0; ind < length; ++ind) {
                    bb.put(tile[ind] ? (byte)1 : (byte) 0);
                }
                return bb.array();
            });

            JavaPairRDD<String, byte[]> compress = compressionManager.compress(rdd8);
            hbaseDAO.bulkSaveRDD(compress, STORAGE_HBASE_GPS + (nbAggr +1), TILES, GPS);

            rdd4.unpersist();
            rdd4 = rdd7aggr;
        }
        rdd4.unpersist();
    }
}
