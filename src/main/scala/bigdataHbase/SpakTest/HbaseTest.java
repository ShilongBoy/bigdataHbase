package bigdataHbase.SpakTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HbaseTest {

    static Configuration conf=HBaseConfiguration.create();
    static  Connection conn;

    static{
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "10.1.69.12,10.1.69.13,10.1.69.11");

        try {

            conn=ConnectionFactory.createConnection(conf);

        } catch (Exception e) {
            // TODO: handle exception
        }
    }


    public static void createTable(String tableNmae){
        try {
            HBaseAdmin admin=(HBaseAdmin)conn.getAdmin();

                HTableDescriptor hDescriptor=new HTableDescriptor(TableName.valueOf(tableNmae));
                HColumnDescriptor hColumnDescriptor=new HColumnDescriptor("info".getBytes());
                hDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hDescriptor);


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    //查看已有表
    public static void listTables() {

        try {
            HBaseAdmin admin=(HBaseAdmin)conn.getAdmin();

            HTableDescriptor hTableDescriptors[] = admin.listTables();
            for(HTableDescriptor hTableDescriptor :hTableDescriptors){
                System.out.println(hTableDescriptor.getNameAsString());
            }

        }catch (IOException e){
            e.printStackTrace();

        }

    }

    public static void main(String[] args) {
        listTables();
//		createTable("tb_test");
//        System.out.println(conn);
    }
}

