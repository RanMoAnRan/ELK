package com.jing.anli.excel;

import com.jing.anli.Article;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.security.PublicKey;
import java.util.List;

/**
 * @Date 2019/8/3
 */
public class HbaseUtil {


    /**
     * 获取客户端连接对象
     */
    static Connection connection;

    static {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化表
     */
    public static Table initTable(String tableName, String... family) {

        TableName tblName = TableName.valueOf(tableName);
        Table table = null;
        try {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tblName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tblName);
                for (String colFamily : family) {

                    hTableDescriptor.addFamily(new HColumnDescriptor(colFamily));
                }
                admin.createTable(hTableDescriptor);
                admin.close();
            }

            table = connection.getTable(tblName);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 根据rowkey插入数据
     */
    public static void putDataByRowkey(String tableName, String family, String column, String colValue, String rowkey) {

        Table table = initTable(tableName, family);
        try {
            Put put = new Put(rowkey.getBytes());
            put.addColumn(family.getBytes(), column.getBytes(), colValue.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据rowkey查询数据
     */
    public static String queryByRowkey(String tableName, String family, String column, String rowkey) {


        Table table = initTable(tableName, family);
        String strValue = "";
        try {
            Get get = new Get(rowkey.getBytes());
            Result result = table.get(get);
            byte[] value = result.getValue(family.getBytes(), column.getBytes());
            strValue = new String(value);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return strValue;
    }

    public static void batchData(List<Article> articles) {

        for (Article article : articles) {

            HbaseUtil.putDataByRowkey("articles","article","title",article.getTitle(),article.getId());
            HbaseUtil.putDataByRowkey("articles","article","from",article.getFrom(),article.getId());
            HbaseUtil.putDataByRowkey("articles","article","time",article.getTime(),article.getId());
            HbaseUtil.putDataByRowkey("articles","article","readCount",article.getReadCount(),article.getId());
            HbaseUtil.putDataByRowkey("articles","article","content",article.getContent(),article.getId());
        }

    }
}
