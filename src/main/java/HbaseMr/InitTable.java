package HbaseMr;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InitTable {
	
	/**
	 * 创建表
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();// hbase-site.xml文件
		conf.set("hbase.zookeeper.quorum", "hadoop-master:2181,hadoop-slave02:2181,hadoop-slave03:2181");
		// 获取连接
		Connection connection = ConnectionFactory.createConnection(conf);
		// 从连接中获取表管理对象
		Admin admin = connection.getAdmin();

		// 表描述
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("word"));
		// 列簇描述
		HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("col");
		hTableDescriptor.addFamily(hColumnDescriptor1);
		admin.createTable(hTableDescriptor);

		// 表描述
		HTableDescriptor hTableDescriptor2 = new HTableDescriptor(TableName.valueOf("stat"));
		// 列簇描述
		HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("ret");
		hTableDescriptor2.addFamily(hColumnDescriptor2);
		admin.createTable(hTableDescriptor2);
		admin.close();
		
		
		//放数据
		Table table = connection.getTable(TableName.valueOf("word"));
		Put put = new Put(Bytes.toBytes("rk001"));
		put.addColumn(Bytes.toBytes("col"), Bytes.toBytes("line"), Bytes.toBytes("hello tom how are you"));

		Put put2 = new Put(Bytes.toBytes("rk002"));
		put2.addColumn(Bytes.toBytes("col"), Bytes.toBytes("line"), Bytes.toBytes("hi hbase i am study"));
		
		Put put3 = new Put(Bytes.toBytes("rk003"));
		put3.addColumn(Bytes.toBytes("col"), Bytes.toBytes("line"), Bytes.toBytes("hadoop hadoop hello you tom hbase"));
		List<Put> list = new ArrayList();
		list.add(put);
		list.add(put2);
		list.add(put3);
		table.put(list);
		table.close();
		connection.close();
	}


}
