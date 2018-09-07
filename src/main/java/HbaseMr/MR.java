package HbaseMr;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * wordCount   
 * 输入的表：数据从一个hbase表里面 word 一个列簇col 有一个列line 存放的一句话
 * 输出的表： stat 一个列簇 ret,有一个列 存放count，存放到rowkey中
 * @author root
 *
 */

public class MR {
	
	public static class MapTask extends TableMapper<Text, IntWritable>{
		
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			byte[] value2 = value.getValue(Bytes.toBytes("col"), Bytes.toBytes("line"));
			String string = new String(value2);
			String[] split = string.split(" ");
			for (String string2 : split) {
				context.write(new Text(string2), new IntWritable(1));
			}
		}
	}
	
	public static class ReduceTask extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			int sum = 0 ;
			for (IntWritable intWritable : values) {
				sum++;
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			
			put.addColumn(Bytes.toBytes("ret"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
			
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-master,hadoop-slave02,hadoop-slave03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MR.class);
		
		Scan scan = new Scan();
		
		TableMapReduceUtil.initTableMapperJob("word", scan, MapTask.class, Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("stat", ReduceTask.class, job);
		
		boolean b = job.waitForCompletion(true);
		
		System.out.println(b?"程序买毛病":"程序全是毛病");
	}

}
