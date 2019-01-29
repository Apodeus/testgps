package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Properties;

public class HBaseDAO implements Serializable {

    private static final Logger LOGGER = LogManager.getLogger(HBaseDAO.class);

    private final byte[] tableName;
    private final Configuration conf;

    public HBaseDAO(){
        String tableNameProp = "ordonezGPS";
        this.tableName = Bytes.toBytes(tableNameProp);

        Configuration conf = HBaseConfiguration.create();
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableNameProp);
        conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "20000");
        this.conf = conf;
    }

    public void bulkSaveRDD(JavaPairRDD<String, byte[]> rdd, String dirName, byte[] family, byte[] qualifier) throws Exception {
        rdd.sortByKey(Comparator.naturalOrder())
                .mapToPair(tuple -> {
                    byte[] rowKey = Bytes.toBytes(tuple._1);
                    KeyValue kv = new KeyValue(rowKey, family, qualifier, tuple._2);
                    return new Tuple2<>(new ImmutableBytesWritable(rowKey), kv);
                })
                .saveAsNewAPIHadoopFile(dirName, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, conf);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();

        TableName table = TableName.valueOf(tableName);
        loader.doBulkLoad(new Path(dirName), admin, connection.getTable(table), connection.getRegionLocator(table));

        admin.close();
        connection.close();
    }
}
