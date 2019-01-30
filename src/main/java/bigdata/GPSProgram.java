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

        //Ici le but est de determiner dans quelle tuile est le point
        // Ainsi que de determiner, en pourcentage, le décalage depuis le coin supérieur gauche de la tuile
        // pour trouver le bon pixel plus tard.
        // Le shift n'est pas le meme selon si on est Nord/Sud ou Ouest/Est
        JavaPairRDD<String, Value> rdd1 = gpsFile.mapToPair(line -> {
            String[] coords = line.split(",");
            long ns = Long.parseLong(coords[0]);
            long we = Long.parseLong(coords[1]);

            long nsKey = ns / 10000000L; // 10^7
            long weKey = we / 10000000L;

            //Rethink this when equal 0 ....
            float shiftX;
            float shiftY;
            if (nsKey < 0) { // if south
                nsKey = 90 + Math.abs(nsKey);
                shiftY = ((ns / 10000000f) % 1);
            } else {
                nsKey = 90 - nsKey;
                shiftY = 1 - ((Math.abs(ns) / 10000000f) % 1);
            }

            if (weKey < 0) {  //if west
                weKey = 180 - Math.abs(weKey);
                shiftX = 1 - ((Math.abs(we) / 10000000f) % 1);
            } else {
                weKey = 180 + weKey;
                shiftX = (we / 10000000f) % 1;
            }
            Coord k = new Coord((int)weKey, (int)nsKey, 0);
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

        //JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> rdd3 = rdd2.groupByKey(); // ??

        JavaPairRDD<String, ArrayList<T3>> rdd3 = rdd2.combineByKey(tu -> {
            ArrayList<T3> arr = new ArrayList<>();
            T3 t3 = new T3(tu._1, tu._2, 1);
            arr.add(t3);
            return arr;
        }, (arr, pixel) -> {
            boolean isPresent = false;
            for (T3 t3 : arr) {
                if (t3.x == pixel._1 && t3.y == pixel._2) {
                    t3.setValue(t3.value + 1);
                    isPresent = true;
                }
            }
            if (!isPresent) {
                arr.add(new T3(pixel._1, pixel._2, 1));
            }
            return arr;
        }, (arr1, arr2) -> {
            ArrayList<T3> arrFinal = new ArrayList<>();
            for (T3 t : arr1) {
                T3 tmp = null;
                for (T3 t2 : arr2) {
                    if (t.x == t2.x && t.y == t2.y) {
                        t.setValue(t.value + t2.value);
                        tmp = t2;
                        break;
                    }
                }
                arr2.remove(tmp);
                arrFinal.add(t);
            }
            arrFinal.addAll(arr2);
            return arrFinal;
        }); // à cacher


        JavaPairRDD<String, byte[]> rdd5 = rdd3.mapValues(tile -> {
            ByteBuffer bb = ByteBuffer.allocate(tile.size() * 3 * Integer.BYTES);

            for (T3 t : tile) {
                bb.putInt(t.x);
                bb.putInt(t.y);
                bb.putInt(t.value);
            }
            return bb.array();
        });

        JavaPairRDD<String, byte[]> compressRdd5 = compressionManager.compress(rdd5);

        hbaseDAO.bulkSaveRDD(compressRdd5, STORAGE_HBASE_GPS + "0", TILES, GPS);

        for(int nbAggr = 0; nbAggr < 8; ++nbAggr){

            // On commence les aggrégations des résultats précédents, il faut peut etre mettre en cache le rdd4 pour l'envoyer en
            // BDD, similaire au groupeByKey, il faut regarder ce qu'il faut quand on utilise des objets customs pour que ca fonctionne
            // equals ? compareTo ?...
            JavaPairRDD<String, Iterable<Tuple2<String, ArrayList<T3>>>> rdd5Aggr = rdd3.groupBy(t -> {
                String[] coords = t._1.split("-");
                int newX = Integer.valueOf(coords[0]) / 2;
                int newY = Integer.valueOf(coords[1]) / 2;
                return newX + "-" + newY + "-" + (Integer.valueOf(coords[2]) + 1);
            });


            rdd5Aggr.mapValues(v -> {
               v.forEach(t -> {
                   String key = t._1;
                   //Calcul du shift X/Y
                   ArrayList<T3> points = t._2;
                   points.forEach(p -> {
                       p.x = p.x / 2 + shiftX;
                       p.y = p.y / 2 + shiftY;
                   });
                   // Créer une map pour garder en mémoire les points qu'on veut merge...

               });
            });

            // Ici le but est de composer la grande tuile
            // Et de recopier chaque mini tuile dans la grande tuile dans le bon coin.
            JavaPairRDD<String, int[]> rdd6Aggr = rdd5Aggr.mapValues(it -> {
                it.forEach(tuple -> {
                    String[] coords = tuple._1.split("-");
                    int coordX = Integer.parseInt(coords[0]);
                    int coordY = Integer.parseInt(coords[1]);

                    int shiftX = TILE_SIZE * (coordX % 2);
                    int shiftY = TILE_SIZE * (coordY % 2);

                    int[] tile = tuple._2;
                    for (int y = 0; y < TILE_SIZE; ++y) {
                        for (int x = 0; x < TILE_SIZE; ++x) {
                            int indexBigTile = (x + shiftX) + ((y + shiftY) * TILE_SIZE * 2);
                            int currIndex = x + y * TILE_SIZE;
                            newTile[indexBigTile] += tile[currIndex];
                        }
                    }
                });
                return newTile;
            });

            // On va maintenant aggréger les valeurs de la grande tuile vers la tuile de taille normale.
            // Avoir la somme par pixel va permettre de jouer sur les nuances de gris pour plus tard.
            JavaPairRDD<String, int[]> rdd7aggr = rdd6Aggr.mapValues(bigTile -> {
                int[] finalTile = new int[TILE_SIZE * TILE_SIZE];
                for (int y = 0; y < TILE_SIZE; ++y) {
                    for (int x = 0; x < TILE_SIZE; ++x) {
                        int sum = 0;
                        for (int j = y*2; j <= y*2 + 1; ++j) {
                            for (int i = x*2; i <= x*2 + 1; ++i) {
                                sum += bigTile[i + j * TILE_SIZE * 2];
                            }
                        }
                        finalTile[x + y * TILE_SIZE] = sum;
                    }
                }
                return finalTile;
            }).cache();

            JavaPairRDD<String, byte[]> rdd8 = rdd7aggr.mapValues(tile -> {
                int length = tile.length;
                ByteBuffer bb = ByteBuffer.allocate(length * Integer.BYTES);
                for (int ind = 0; ind < length; ++ind) {
                    bb.putInt(tile[ind]);
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
