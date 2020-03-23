package com.asiainfo.hbase.test.tool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.RegionInfo;


public class TestOpenECFile {

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		System.out.println("Start...");
//		if (args.length != 4) {
//			System.out.println("Need 4 parametes: [1.dataPath 2.nameSpaceName 3.tableName 4. confPath], only passed: "
//					+ args.length);
//		}
//		System.out.println("dataPath: " + args[0]);
//		String dataPath = args[0];
//		System.out.println("nameSpaceName: " + args[1]);
//		String nameSpaceName = args[1];
//		System.out.println("tableName: " + args[2]);
//		String tableName = args[2];
//		System.out.println("confPath: " + args[3]);
//		String confPath = args[3];

		// String dataPath = "/apps/hbase/data4/data/";
		// String nameSpaceName = "default";
		// String tableName = "zhaoym";
		// String confPath = "/Users/zhaoyim/eclipse/workspace/hbasers/hbasers";
//		ConfProperties.setPath(confPath);
//
//		System.setProperty("java.security.krb5.conf",
//				ConfProperties.getConf().get("java.security.krb5.conf").toString());
//
//		Configuration conf = new Configuration();
//		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//		conf.set("hadoop.security.authentication", "kerberos");
//
//		conf.set("hbase.zookeeper.quorum", ConfProperties.getConf().get("hbase.zookeeper.quorum").toString());
//		conf.set("hbase.zookeeper.property.clientPort",
//				ConfProperties.getConf().get("hbase.zookeeper.property.clientPort").toString());
//		conf.set("zookeeper.znode.parent", ConfProperties.getConf().get("zookeeper.znode.parent").toString());
//		conf.set("fs.defaultFS", ConfProperties.getConf().get("fs.defaultFS").toString());
//
//		UserGroupInformation.setConfiguration(conf);
//		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
//				ConfProperties.getConf().get("krb.user.principle").toString(),
//				ConfProperties.getConf().get("krb.user.keytab").toString());
//
//		
//		FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
//			public FileSystem run() throws IOException {
//				return FileSystem.get(conf);
//			}
//		});
		
		Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.quorum",
		// "kvm-dp-centos76-node2,kvm-dp-centos76-node3,kvm-dp-centos76-node4");
		conf.set("hbase.zookeeper.quorum", "host-10-1-236-55,host-10-1-236-56,host-10-1-236-57");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure1");
		// conf.set("hbase.bulkload.retries.number", "10000");
		System.setProperty("HADOOP_USER_NAME", "ocdp");
		conf.set("fs.defaultFS", "hdfs://10.1.236.55:8020");

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		FileSystem fs = FileSystem.get(new URI("/tmp"), conf, "ocdp");
		
		
		List<FSDataInputStream> list = new ArrayList<FSDataInputStream>();
//		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path("/zhaoyim"), false);
		
//		for(int i = 1; i<10000; i++){
			String fileName = "/tmp/zhaoyim/test.data";
			System.out.println(fileName);
//			readFile(fs, list, "/zhaoyim/test" + i + ".data");
			Path path = new Path(fileName);
			FSDataInputStream fsDataInputStream = fs.open(path);
//			list.add(fsDataInputStream);
			byte[] bytes = new byte[1024];
	        int len = -1;
	        ByteArrayOutputStream stream = new ByteArrayOutputStream();
	 
	        while ((len = fsDataInputStream.read(bytes)) != -1) {
	            stream.write(bytes, 0, len);
	        }
	        fsDataInputStream.close();
	        stream.close();
	        System.out.println(new String(stream.toByteArray()));
//	        fsDataInputStream.unbuffer();
//		}
		
//		while (ri.hasNext()) {
//			LocatedFileStatus fileStatus = ri.next();
//			Path fullPath = fileStatus.getPath();
//			System.out.println(fullPath.getName());
//			if (!fullPath.getName().contains("test.data")) {
//				readFile(fs, list, "/zhaoyim/" + fullPath.getName());
//			}
//		}
		
//		readFile(fs, list, "/zhaoyim/" + "test1.data");

		
//		List<String> metaRegions = getMetaRegions(conf, nameSpaceName, tableName, ugi);
//
//		Map<String, RegionInfo> hdfsRegions = getHdfsRegions(conf, dataPath, nameSpaceName, tableName, ugi);
//
//		Set<String> hdfsRegionNames = hdfsRegions.keySet();
//		hdfsRegionNames.removeAll(new HashSet<>(metaRegions));
//		System.out.println("hdfsRegionNames size: " + hdfsRegionNames.size());
//		fixHbaseMeta(ugi, conf, hdfsRegionNames, hdfsRegions);

		System.out.println("End...");

	}
	
//	private static void readFile(FileSystem fs, List<FSDataInputStream> list, String fileName) throws IOException{
//		Path path = new Path(fileName);
//		FSDataInputStream fsDataInputStream = fs.open(path);
//		list.add(fsDataInputStream);
//		byte[] bytes = new byte[1024];
//        int len = -1;
//        ByteArrayOutputStream stream = new ByteArrayOutputStream();
// 
//        while ((len = fsDataInputStream.read(bytes)) != -1) {
//            stream.write(bytes, 0, len);
//        }
////        fsDataInputStream.close();
//        stream.close();
//        System.out.println(new String(stream.toByteArray()));
//        fsDataInputStream.unbuffer();
//	}

}
