package utils;

import com.dataartisans.flink_demo.datatypes.WasteRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Connect2HBase {
    private Table table = null;
    public Connect2HBase(String zkQuorum, String port) throws IOException {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", zkQuorum);
        config.set("hbase.zookeeper.property.clientPort", port);
//        config.set("zookeeper.znode.parent", parent);
//        config.set("hbase.master", "localhost:16000");
        Connection connection = ConnectionFactory.createConnection(config);

        this.table = connection.getTable(TableName.valueOf("totalmoney"));
    }
    public void putData(String id, String money) throws IOException {

        Put p = new Put(Bytes.toBytes(id));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(Bytes.toBytes("moneyfamily"), Bytes.toBytes("money"), Bytes.toBytes(money));

        this.table.put(p);
    }
    public String getData(String rowname) throws IOException {
        Get g = new Get(Bytes.toBytes(rowname));

        // Reading the data
        Result result = table.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("moneyfamily"),Bytes.toBytes("money"));

        String money = Bytes.toString(value);
        return money;
    }
    public void close() throws IOException {
        this.table.close();
    }
}

