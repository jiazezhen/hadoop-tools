package com.asiainfo.hbase.test.tool;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHbaseBlukLoad {

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		System.out.println("Start...");

		System.out.println("hfileDir: " + args[0]);
		String hfileDir = args[0];
		System.out.println("CFName: " + args[1]);
		String CFName = args[1];
		System.out.println("hfileName: " + args[2]);
		String hfileName = args[2];
		System.out.println("tableName: " + args[3]);
		String tableNameStr = args[3];
		System.out.println("rowsNumber: " + args[4]);
		String rowsNumber = args[4];
		System.out.println("rowSize: " + args[5]);
		String rowSize = args[5];
		System.out.println("splits: " + args[6]);
		String splits = args[6];
		System.out.println("regionSplitCount: " + args[7]);
		String regionSplitCount = args[7];

		List<String> list = new ArrayList<String>();
		int total = Integer.parseInt(rowsNumber);
		for (int i = 0; i < total; i++) {
			list.add(String.valueOf(i));
		}
		// sort
		Collections.sort(list);
		System.out.println("end generate row keys");

		Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.quorum",
		// "kvm-dp-centos76-node2,kvm-dp-centos76-node3,kvm-dp-centos76-node4");
		conf.set("hbase.zookeeper.quorum", "host-10-1-236-55,host-10-1-236-56,host-10-1-236-57");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
		// conf.set("hbase.bulkload.retries.number", "10000");
		System.setProperty("HADOOP_USER_NAME", "ocdp");
		conf.set("fs.defaultFS", "hdfs://10.1.236.55:8020");

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		FileSystem fs = FileSystem.get(new URI("/tmp"), conf, "ocdp");

		HFileContext hfileContext = new HFileContextBuilder().withCompression(Compression.Algorithm.NONE).build();
		Path hfilePathWithName = new Path(hfileDir + File.separator + CFName + File.separator + hfileName);
		HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, hfilePathWithName)
				.withFileContext(hfileContext).create();
		try {

			// create folder
			// fs.mkdirs(new Path(hfileDir + File.separator + CFName));

			// create table
			TableName tableName = TableName.valueOf(tableNameStr);
			TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
			ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"));
			ColumnFamilyDescriptor cfd = cdb.build();
			tdb.setColumnFamily(cfd);
			TableDescriptor td = tdb.build();
			int len = Integer.parseInt(splits);
			byte[][] splitKeys = new byte[len][];
			for (int j = 0; j < len; j++) {
				if (j < 10) {
					splitKeys[j] = Bytes.toBytes("00" + j);
					continue;
				}
				if (j < 100) {
					splitKeys[j] = Bytes.toBytes("0" + j);
					continue;
				}
//				if (j < 1000) {
//					splitKeys[j] = Bytes.toBytes("0" + j);
//					continue;
//				} 
				splitKeys[j] = Bytes.toBytes(String.valueOf(j));
			}
			System.out.println("end create splitKeys");
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("delete exist table");
			}
			admin.createTable(td, splitKeys);
			System.out.println("end create table");

			// generate hfile
			int count = 0;
			int fileCount = 0;
			int flag = Integer.parseInt(regionSplitCount);
			for (String s : list) {
				if (count > flag) {
					writer.close();
					hfilePathWithName = new Path(
							hfileDir + File.separator + CFName + File.separator + hfileName + fileCount);
					writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, hfilePathWithName)
							.withFileContext(hfileContext).create();
					count = 0;
					fileCount++;
				}

				KeyValue keyValue = new KeyValue(Bytes.toBytes(s), Bytes.toBytes("cf"), Bytes.toBytes("col1"),
						Bytes.toBytes(RandomStringUtils.random(Integer.parseInt(rowSize))));
				writer.append(keyValue);
				count++;
			}
			writer.close();
			fs.close();
			System.out.println("end generate hfile");

			// load hfile
			Table table = conn.getTable(tableName);
			RegionLocator locator = conn.getRegionLocator(tableName);

			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			Path path = new Path("hdfs://10.1.236.55:8020" + hfileDir);
			loader.doBulkLoad(path, admin, table, locator);
			System.out.println("end load hfile");

		} catch (Exception e) {
			System.out.println("Exception");
			System.out.println(e.getStackTrace());
			System.out.println(e.getCause());
			System.out.println(e.getMessage());
		} finally {
			// writer.close();
			fs.close();
			conn.close();
			admin.close();
		}
		System.out.println("End");
	}

}
