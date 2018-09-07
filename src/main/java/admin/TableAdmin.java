package admin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

public class TableAdmin {
	
	
	@SuppressWarnings("deprecation")
	@Test
	public void testCreateTable() throws Exception {
		
		/*HBaseConfiguration conf = new HBaseConfiguration();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");*/
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin admin = conn.getAdmin();
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("t_u"));
		
		HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("base_info");
		hColumnDescriptor1.setMaxVersions(3);
		
		HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("family");
		
		hTableDescriptor.addFamily(hColumnDescriptor1);
		hTableDescriptor.addFamily(hColumnDescriptor2);
		admin.createTable(hTableDescriptor);
		
		admin.close();
		conn.close();
		
	}
	
	@Test
	public void testDropTable() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin admin = conn.getAdmin();
		
		admin.disableTable(TableName.valueOf("t_u"));
		admin.deleteTable(TableName.valueOf("t_u"));
		admin.close();
		conn.close();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testAlterTable() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin admin = conn.getAdmin();
		
		HTableDescriptor user = admin.getTableDescriptor(TableName.valueOf("t_u"));
		
		HColumnDescriptor f3 = new HColumnDescriptor("f3");
		user.addFamily(f3);
		
		admin.modifyTable(TableName.valueOf("t_u"), user);
		
		admin.close();
		conn.close();
		
	}
	
	

}
