package admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class DataCRUD {
	
	@Test
	public void testPutData() throws Exception {
		
		Configuration create = HBaseConfiguration.create();
		create.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		create.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(create);
		
		Table table = conn.getTable(TableName.valueOf("t_u"));
		
		Put put1 = new Put(Bytes.toBytes("rk001"));
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"), Bytes.toBytes("lisi"));
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		
		Put put2 = new Put(Bytes.toBytes("rk002"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"), Bytes.toBytes("zhangsan"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		Put put3 = new Put(Bytes.toBytes("rk003"));
		put3.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"), Bytes.toBytes("wangwu"));
		put3.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		Put put4 = new Put(Bytes.toBytes("rk004"));
		put4.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"), Bytes.toBytes("zhaoliu"));
		put4.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		Put put5 = new Put(Bytes.toBytes("rk005"));
		put5.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"), Bytes.toBytes("zhangsan"));
		put5.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		 
	
		List<Put> list = new ArrayList();
		list.add(put1);
		list.add(put2);
		list.add(put3);
		list.add(put4);
		list.add(put5);
		
		
		table.put(list);
		table.close();
		conn.close();
}
	
	@Test
	public void testDeleteData() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf("t_user"));
		
		Delete delete = new Delete(Bytes.toBytes("rk001"));
		delete.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"));
		table.delete(delete);
		
		table.close();
		conn.close();
		
	}
	
}
