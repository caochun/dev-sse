package utils;

import com.dataartisans.flink_demo.datatypes.WasteRecord;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WasteHbase {
    private Table table = null;
    private Table tableout = null;
    public WasteHbase(String zkQuorum, String port) throws IOException {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", zkQuorum);
        config.set("hbase.zookeeper.property.clientPort", port);
//        config.set("zookeeper.znode.parent", parent);
//        config.set("hbase.master", "localhost:16000");
        Connection connection = ConnectionFactory.createConnection(config);

        this.table = connection.getTable(TableName.valueOf("inwastetable"));
        this.tableout = connection.getTable(TableName.valueOf("outwastetable"));
    }
    public void putData(WasteRecord w) throws IOException {

        Put p = new Put(Bytes.toBytes(w.vlp()));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(Bytes.toBytes("station"), Bytes.toBytes("stationid"), Bytes.toBytes(w.stationId()));
        p.addColumn(Bytes.toBytes("station"), Bytes.toBytes("stationname"), Bytes.toBytes(w.stationName()));


        // Saving the put Instance to the HTable.
        this.table.put(p);
    }
    public void putInData(WasteRecord w) throws IOException {

        Put p = new Put(Bytes.toBytes(w.vlp()));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(Bytes.toBytes("station"), Bytes.toBytes("inf"), Bytes.toBytes(w.toString()));
        this.table.put(p);
    }
    public void putOutData(WasteRecord w) throws IOException {

        Put p = new Put(Bytes.toBytes(w.wasteId()));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("info"), Bytes.toBytes(w.toString()));
        this.tableout.put(p);
    }
    public String getData(String rowname) throws IOException {
        Get g = new Get(Bytes.toBytes(rowname));

        // Reading the data
        Result result = table.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("station"),Bytes.toBytes("stationid"));

        String stationid = Bytes.toString(value);
        return stationid;
    }
    public String getInData(String wasteid) throws IOException{
        Get g = new Get(Bytes.toBytes(wasteid));

        // Reading the data
        Result result = table.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("station"),Bytes.toBytes("inf"));

        String inf = Bytes.toString(value);
        return inf;
    }
    public String getOutData(String wasteid) throws IOException{
        Get g = new Get(Bytes.toBytes(wasteid));

        // Reading the data
        Result result = tableout.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(Bytes.toBytes("inf"),Bytes.toBytes("info"));

        String inf = Bytes.toString(value);
        return inf;
    }
    public void close() throws IOException {
        this.table.close();
        this.tableout.close();
    }
}
