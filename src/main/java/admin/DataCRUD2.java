package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataCRUD2 {
	
	Table table = null;
	Connection conn = null;
	
	@Before
	public void init() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conn = ConnectionFactory.createConnection(conf);
		table = conn.getTable(TableName.valueOf("t_u"));
	}
	
	/**
	 * 获取一行的数据
	 * @throws Exception
	 */
	@Test
	public void getData() throws Exception {
		Get get = new Get(Bytes.toBytes("rk001"));
		Result result = table.get(get);
		byte[] age = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
		System.out.println(Bytes.toString(age));
		byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"));
		System.out.println(Bytes.toString(value));
		conn.close();
		table.close();
		
	}
	
	
	/**
	 * 得到多行的数据
	 * @throws Exception
	 */
	@Test
	public void getDatas() throws Exception {
		
		Scan scan = new Scan(Bytes.toBytes("rk002"), Bytes.toBytes("rk005"));//2-4  不包括5
		
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"));
			System.out.println(Bytes.toString(value));
			byte[] age = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(age));
			
			System.out.println("--------------------");
		}
	}
	/**
	 * 列值过滤器
	 * @throws Exception 
	 */
	@Test
	public void ScanData() throws Exception {
		Scan scan = new Scan();
		
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"), CompareOp.EQUAL, Bytes.toBytes("zhangsan"));		
		
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"));
			System.out.println(Bytes.toString(value));
			byte[] age = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(age));
			System.out.println("-----------------------");
		}
	}
	
	
	/**
	 * 列名前缀过滤器
	 * @throws Exception 
	 * @throws Exception
	 */
	@Test
	public void testPrefixFielter() throws Exception {
		Scan scan = new Scan();
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));
		scan.setFilter(columnPrefixFilter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"));
			System.out.println(Bytes.toString(value));
			System.out.println("-----------------------");
		}
	}
	
	/**
	 * 行过滤器
	 * @throws Exception 
	 * @throws Exception
	 */
	@Test
	public void testRowFilter() throws Exception {
		
		Scan scan = new Scan();
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,  new RegexStringComparator("^rk"));//匹配以rk开头的行
		scan.setFilter(rowFilter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"));
			System.out.println(Bytes.toString(value));
			byte[] age = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(age));
			System.out.println("-----------------------");
		}
	}
	
	@Test
	public void testListFilter() throws Exception {
		Scan scan = new Scan();
		
		FilterList list = new FilterList();
		
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,   new RegexStringComparator("^rk"));
		list.addFilter(rowFilter);
		list.addFilter(columnPrefixFilter);
		scan.setFilter(list);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name_2"));
			System.out.println(Bytes.toString(value));
			System.out.println("-----------------------");
		}
	}
	
	@After
	public void close() throws Exception {
		conn.close();
		table.close();
	}

}
