package com.asiainfo.hbase.hbasers;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * Hello world!
 */
public class FixHbaseMeta {

    private static final String HELP = "h";
    private static final String DATA_PATH = "d";
    private static final String NAMESPACE = "n";
    private static final String TABLE_NAME = "t";
    private static final String CONFIG_PATH = "c";
    private static final String KERBEROS_ENABLE = "k";
    private static final String DEFAULT_NAMESPACE = "default";

    public static void main(String[] args) throws IOException, InterruptedException {
        cmdLineBegin(args);
    }

    private static void cmdLineBegin(String[] args) throws IOException, InterruptedException {
        Options options = new Options();

        options.addOption(Option.builder(HELP)
                .longOpt("help")
                .desc("show this help message and exit program")
                .build());


        options.addOption(Option.builder(DATA_PATH)
                .longOpt("data")
                .hasArg()
                .argName("datapath")
                .required(true)
                .desc("The metadata path of HBase on HDFS" +
                        "\ne.g /apps/hbase/data/data/")
                .build());

        options.addOption(Option.builder(NAMESPACE)
                .longOpt("namespace")
                .hasArg()
                .argName("namespace")
                .required(true)
                .desc("The namespace of table" +
                        "\ne.g default hbase ")
                .build());

        options.addOption(Option.builder(TABLE_NAME)
                .longOpt("tablename")
                .hasArg()
                .argName("tablename")
                .required(true)
                .desc("Which table's metadata to fix")
                .build());

        options.addOption(Option.builder(CONFIG_PATH)
                .longOpt("config")
                .hasArg()
                .argName("configpath")
                .required(true)
                .desc("Path to configuration.properties" +
                        "\ne.g /root/conf/")
                .build());

        options.addOption(Option.builder(KERBEROS_ENABLE)
                .longOpt("kerberos")
                .desc("If the cluster has Kerberos enabled, this is required")
                .build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine result = null;

        try {
            result = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("FixHbaseMeta", options, true);
            System.exit(1);
        }

        if (result.hasOption(HELP)) {
            formatter.printHelp("FixHbaseMeta", options, true);
            System.exit(0);
        }

        if (result.hasOption(DATA_PATH) && result.hasOption(NAMESPACE) && result.hasOption(TABLE_NAME) && result.hasOption(CONFIG_PATH)) {
            System.out.println("datapath=" + result.getOptionValue(DATA_PATH)
                    + "\nnamespace=" + result.getOptionValue(NAMESPACE)
                    + "\ntablename=" + result.getOptionValue(TABLE_NAME)
                    + "\nconfigpath=" + result.getOptionValue(CONFIG_PATH));

            kerberosSwitch(result);


        } else {
            formatter.printHelp("FixHbaseMeta", options, true);
        }


    }

    private static void kerberosSwitch(CommandLine result) throws IOException, InterruptedException {
        String nameSpaceName = result.getOptionValue(NAMESPACE);
        String dataPath = result.getOptionValue(DATA_PATH);
        String tableName = result.getOptionValue(TABLE_NAME);

        Configuration conf = HBaseConfiguration.create();
        ConfProperties.setPath(result.getOptionValue("c"));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", ConfProperties.getConf().get("fs.defaultFS").toString());
        conf.set("hbase.zookeeper.quorum", ConfProperties.getConf().get("hbase.zookeeper.quorum").toString());
        conf.set("hbase.zookeeper.property.clientPort",
                ConfProperties.getConf().get("hbase.zookeeper.property.clientPort").toString());
        conf.set("zookeeper.znode.parent", ConfProperties.getConf().get("zookeeper.znode.parent").toString());

        Connection conn;
        FileSystem fs;

        if (result.hasOption(KERBEROS_ENABLE)) {
            System.out.println("kerberos is enabled");
            System.setProperty("java.security.krb5.conf",
                    ConfProperties.getConf().get("java.security.krb5.conf").toString());
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                    ConfProperties.getConf().get("krb.user.principle").toString(),
                    ConfProperties.getConf().get("krb.user.keytab").toString());
            conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws IOException {
                    return ConnectionFactory.createConnection(conf);
                }
            });

            fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws IOException {
                    return FileSystem.get(conf);
                }
            });


        } else {
            System.out.println("kerberos is disabled");
            conn = ConnectionFactory.createConnection(conf);
            fs = FileSystem.get(conf);
        }

        List<String> metaRegions = getMetaRegions(conn, nameSpaceName, tableName);

        Map<String, RegionInfo> hdfsRegions = getHdfsRegions(fs, dataPath, nameSpaceName, tableName);

        Set<String> hdfsRegionNames = hdfsRegions.keySet();
        hdfsRegionNames.removeAll(new HashSet<>(metaRegions));
        System.out.println("hdfsRegionNames size: " + hdfsRegionNames.size());

        fixHbaseMeta(conn, hdfsRegionNames, hdfsRegions);

        System.out.println("End...");


    }

    private static void fixHbaseMeta(Connection conn, Set<String> hdfsRegionNames,
                                     Map<String, RegionInfo> hdfsRegions) throws IOException, InterruptedException {

        try {
            Table table = conn.getTable(TableName.valueOf("hbase:meta"));

            Collection<ServerName> regionServers = conn.getAdmin().getRegionServers();
            Object[] rss = regionServers.toArray();
            System.out.println("regionServers: " + rss.length);

            for (String name : hdfsRegionNames) {
                ServerName rs = (ServerName) rss[new Random().nextInt(regionServers.size())];
                System.out.println("select regionServer: " + rs);
                RegionInfo hri = hdfsRegions.get(name);
                Put info = MetaTableAccessor.makePutFromRegionInfo(hri, EnvironmentEdgeManager.currentTime());
                info.addColumn("info".getBytes(), "sn".getBytes(), Bytes.toBytes(rs.getServerName()));
                info.addColumn("info".getBytes(), "server".getBytes(), Bytes.toBytes(rs.getAddress().toString()));
                info.addColumn("info".getBytes(), "state".getBytes(), Bytes.toBytes("OPEN"));
                table.put(info);
                System.out.println("Put completed!");
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fixHbaseMeta hit exception: " + e);
        } finally {
            conn.close();
        }
    }

    private static List<String> getMetaRegions(Connection conn, String nameSpaceName, String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf("hbase:meta"));
        try {
            System.out.println("get table.");
            PrefixFilter filter = null;
            if (DEFAULT_NAMESPACE.equals(nameSpaceName)) {
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
                metaRegions.add(Bytes.toStringBinary(result.getRow()));
                System.out.println("data ==>" + Bytes.toStringBinary(result.getRow()));
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
            //conn.close();
        }
    }


    private static Map<String, RegionInfo> getHdfsRegions(FileSystem fs, String dataPath, String nameSpaceName,
                                                          String tableName) throws IOException, InterruptedException {
        try {
            // TODO should check the datapath if end with / should remove
            // "/ocdp1/apps/hbase/data4/data/"
            Path path = new Path(dataPath + nameSpaceName + File.separator + tableName);
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
