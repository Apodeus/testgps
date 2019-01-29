package bigdata;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressionManager implements Serializable {

    /**
     * Compress the byte array
     * @param rdd
     * @return
     */
    public JavaPairRDD<String, byte[]> compress(JavaPairRDD<String, byte[]> rdd){
        return rdd.mapToPair(tuple -> {
            CompressionManager zipper = new CompressionManager();
            byte[] compressedHeightMap = zipper.compress(tuple._2);
            return new Tuple2<>(tuple._1, compressedHeightMap);
        });
    }

    private byte[] compress(byte[] data) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        deflater.finish();
        byte[] buffer = new byte[2048];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }

    private byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[2048];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }
}