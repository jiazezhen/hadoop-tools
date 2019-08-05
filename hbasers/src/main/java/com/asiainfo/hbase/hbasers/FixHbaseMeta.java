package com.asiainfo.hbase.hbasers;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hello world!
 *
 */
public class FixHbaseMeta {
	public static void main(String[] args) throws Exception {
		System.out.println("Start...");
		if (args.length != 4) {
			System.out.println("Need 4 parametes: [1.dataPath 2.nameSpaceName 3.tableName 4. confPath], only passed: "
					+ args.length);
		}
		System.out.println("dataPath: " + args[0]);
		String dataPath = args[0];
		System.out.println("nameSpaceName: " + args[1]);
		String nameSpaceName = args[1];
		System.out.println("tableName: " + args[2]);
		String tableName = args[2];
		System.out.println("confPath: " + args[3]);
		String confPath = args[3];

		// String dataPath = "/apps/hbase/data4/data/";
		// String nameSpaceName = "default";
		// String tableName = "zhaoym";
		// String confPath = "/Users/zhaoyim/eclipse/workspace/hbasers/hbasers";
		ConfProperties.setPath(confPath);

		System.setProperty("java.security.krb5.conf",
				ConfProperties.getConf().get("java.security.krb5.conf").toString());

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("hadoop.security.authentication", "kerberos");

		conf.set("hbase.zookeeper.quorum", ConfProperties.getConf().get("hbase.zookeeper.quorum").toString());
		conf.set("hbase.zookeeper.property.clientPort",
				ConfProperties.getConf().get("hbase.zookeeper.property.clientPort").toString());
		conf.set("zookeeper.znode.parent", ConfProperties.getConf().get("zookeeper.znode.parent").toString());
		conf.set("fs.defaultFS", ConfProperties.getConf().get("fs.defaultFS").toString());

		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
				ConfProperties.getConf().get("krb.user.principle").toString(),
				ConfProperties.getConf().get("krb.user.keytab").toString());

		List<String> metaRegions = getMetaRegions(conf, nameSpaceName, tableName, ugi);

		Map<String, RegionInfo> hdfsRegions = getHdfsRegions(conf, dataPath, nameSpaceName, tableName, ugi);

		Set<String> hdfsRegionNames = hdfsRegions.keySet();
		hdfsRegionNames.removeAll(new HashSet<>(metaRegions));
		System.out.println("hdfsRegionNames size: " + hdfsRegionNames.size());
		fixHbaseMeta(ugi, conf, hdfsRegionNames, hdfsRegions);

		System.out.println("End...");
	}

	private static void fixHbaseMeta(UserGroupInformation ugi, Configuration conf, Set<String> hdfsRegionNames,
			Map<String, RegionInfo> hdfsRegions) throws IOException, InterruptedException {
		Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
			public Connection run() throws IOException {
				return ConnectionFactory.createConnection(conf);
			}
		});
		try {
			Table table = conn.getTable(TableName.valueOf("hbase:meta"));
			// Table table =
			// conn.getTable(TableName.valueOf("default:hbase_test5"));

			Collection<ServerName> regionServers = conn.getAdmin().getRegionServers();
			Object[] rss = regionServers.toArray();
			System.out.println("regionServers: " + rss.length);

			for (String name : hdfsRegionNames) {
				ServerName rs = (ServerName) rss[new Random().nextInt(regionServers.size())];
				System.out.println("select regionServer: " + rs);
				RegionInfo hri = hdfsRegions.get(name);
				// Thread.sleep(500);
				Put info = MetaTableAccessor.makePutFromRegionInfo(hri, EnvironmentEdgeManager.currentTime());
				info.addColumn("info".getBytes(), "sn".getBytes(), Bytes.toBytes(rs.getServerName()));
				info.addColumn("info".getBytes(), "server".getBytes(), Bytes.toBytes(rs.getAddress().toString()));
				info.addColumn("info".getBytes(), "state".getBytes(), Bytes.toBytes("OPEN"));
				table.put(info);
				System.out.println("Put completed!");
			}
			table.close();
			conn.close();
		} catch (Exception e) {
			System.out.println("fixHbaseMeta hit exception: " + e);
		} finally {
			conn.close();
		}
	}

	private static List<String> getMetaRegions(Configuration conf, String nameSpaceName, String tableName,
			UserGroupInformation ugi) throws IOException, InterruptedException {
		Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
			public Connection run() throws IOException {
				return ConnectionFactory.createConnection(conf);
			}
		});
		try {
			Table table = conn.getTable(TableName.valueOf("hbase:meta"));
			System.out.println("get table.");
			PrefixFilter filter = null;
			if (nameSpaceName.equals("default")) {
				filter = new PrefixFilter(Bytes.toBytes(tableName + ","));
				System.out.println("set filter in default ns.");
			} else {
				filter = new PrefixFilter(Bytes.toBytes(nameSpaceName + ":" + tableName + ","));
				System.out.println("set filter.");
			}

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
			return metaRegions;
		} catch (Exception e) {
			System.out.println("getMetaRegions: " + e);
			System.out.println("getMetaRegions: " + e.getMessage());
			System.out.println("getMetaRegions: " + e.getStackTrace().toString());
			System.out.println("getMetaRegions: " + e.getCause());
			return new ArrayList<String>();
		} finally {
			conn.close();
		}
	}

	private static Map<String, RegionInfo> getHdfsRegions(Configuration conf, String dataPath, String nameSpaceName,
			String tableName, UserGroupInformation ugi) throws IOException, InterruptedException {
		FileSystem fs = null;
		try {
			fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
				public FileSystem run() throws IOException {
					return FileSystem.get(conf);
				}
			});
			// TODO should check the datapath if end with / should remove
			Path path = new Path(dataPath + nameSpaceName + File.separator + tableName);// "/ocdp1/apps/hbase/data4/data/"
			// + tablePath);
			Map<String, RegionInfo> hdfsRegions = new HashMap<String, RegionInfo>();
			FileStatus[] list = fs.listStatus(path);
			System.out.println("list size: " + list.length);
			for (FileStatus status : list) {
				if (!status.isDirectory()) {
					continue;
				}
				boolean isRegion = false;
				FileStatus[] regions = fs.listStatus(status.getPath());
				System.out.println("hdfs path: " + status.getPath().getName());
				for (FileStatus regionStatus : regions) {
					if (regionStatus.toString().contains(".regioninfo")) {
						System.out.println("region path: " + status.getPath().getName());
						isRegion = true;
						break;
					}
				}
				if (!isRegion) {
					continue;
				}
				RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, status.getPath());
				hdfsRegions.put(hri.getRegionNameAsString(), hri);
			}
			System.out.println("HDFS region size: " + hdfsRegions.size());
			return hdfsRegions;
		} catch (Exception e) {
			System.out.println("getHdfsRegions: " + e);
			return new HashMap<String, RegionInfo>();
		} finally {
			fs.close();
		}
	}
}
