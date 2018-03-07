package site.it4u.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase操作工具类 采用单例模式封装
 */
public class HBaseUtils {

    HBaseAdmin admin = null;

    Configuration configuration = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase;zookeeper.quorum", "hadoop:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop:8020/hbase");
        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if(instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取Htable示例
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
          table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 增加一条记录到hbase表
     * @param tableName 表名
     * @param rowKey 表的rowkey
     * @param cf 表的columnfamily
     * @param column 表的列
     * @param value 写入的值
     */
    public void put(String tableName, String rowKey, String cf, String column, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
