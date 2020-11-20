package com.hadoop.hbase.test.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHbasePut {

	public static void main(String[] args) throws IOException, InterruptedException {
		
//		System.out.println(Runtime.getRuntime().availableProcessors());
//		System.out.println("dataPath: " + args[0]);
//		String dataPath = args[0];
//		System.out.println("nameSpaceName: " + args[1]);
//		String nameSpaceName = args[1];
//		System.out.println("tableName: " + args[2]);
//		String tableName = args[2];
//		System.out.println("confPath: " + args[3]);
//		String confPath = args[3];
//		
//		ConfProperties.setPath(confPath);

//		System.setProperty("java.security.krb5.conf",
//				ConfProperties.getConf().get("java.security.krb5.conf").toString());

		Configuration conf = new Configuration();
//		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//		conf.set("hadoop.security.authentication", "kerberos");

//		conf.set("hbase.zookeeper.quorum", "kvm-dp-centos76-node2,kvm-dp-centos76-node3,kvm-dp-centos76-node4");
		conf.set("hbase.zookeeper.quorum", "host-10-1-236-60");
		
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
//		conf.set("fs.defaultFS", ConfProperties.getConf().get("fs.defaultFS").toString());

//		UserGroupInformation.setConfiguration(conf);
//		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
//				ConfProperties.getConf().get("krb.user.principle").toString(),
//				ConfProperties.getConf().get("krb.user.keytab").toString());
//
//		
//		Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
//			public Connection run() throws IOException {
//				return ConnectionFactory.createConnection(conf);
//			}
//		});
		
		Connection conn = ConnectionFactory.createConnection(conf);
		
//		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		TableName[] tableNames = admin.listTableNames();
	    for (TableName tableName1 : tableNames) {
	        System.out.println(tableName1);
	    }
	    
	    Table table = conn.getTable(TableName.valueOf("hbase:meta"));
		System.out.println("get table.");
		PrefixFilter filter = null;
//		if (nameSpaceName.equals("default")) {
			filter = new PrefixFilter(Bytes.toBytes("zhaoyim001" + ","));
//			System.out.println("set filter in default ns.");
//		} else {
//			filter = new PrefixFilter(Bytes.toBytes(nameSpaceName + ":" + tableName + ","));
//			System.out.println("set filter.");
//		}

		Scan scan = new Scan();
		scan.setFilter(filter);

		List<String> metaRegions = new ArrayList<String>();

		System.out.println("start get data.");
		Iterator<Result> iterator = table.getScanner(scan).iterator();
		System.out.println("end get data.");
		while (iterator.hasNext()) {
			System.out.println("start loop data ==>" + iterator.hasNext());
			Result result = iterator.next();
			metaRegions.add(Bytes.toString(result.getRow()));
			System.out.println("data ==>" + result.getRow().toString());
		}
		table.close();
		System.out.println("metaRegions size: " + metaRegions.size());
		
//		Collection<ServerName> regionServers = conn.getAdmin().getRegionServers();
//		Table table = conn.getTable(TableName.valueOf("default:zhaoyim001"));
//		List<Put> list =new ArrayList<Put>();
//		String rowkey = "row1";
//		String cf = "cf";
//		String column = "col1";
//		String content = "TestString";
//		Put put = new Put(Bytes.toBytes(rowkey));
//		put.setDurability(Durability.SKIP_WAL);
//		put.addColumn(cf.getBytes(), column.getBytes(), content.getBytes());
//		list.add(put);
//		table.put(list);
//		table.close();
//		conn.close();
	}

}
