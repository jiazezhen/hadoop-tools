package com.hadoop.hbase.test.tool;

import com.hadoop.hbase.fixmeta.ConfProperties;
import org.apache.commons.cli.*;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TestHbaseBlukLoad {

    private static final String HELP = "h";
    private static final String HFILE_DIR = "d";
    private static final String CF_NAME = "cf";
    private static final String HFILE_NAME = "fn";
    private static final String TABLE_NAME = "t";
    private static final String ROWS_NUMBER = "rn";
    private static final String ROW_SIZE = "rs";
    private static final String SPLITS = "s";
    private static final String REGION_SPLIT_COUNT = "rsc";
    private static final String CONFIG_PATH = "c";
    private static final String KERBEROS_ENABLE = "k";
    private static final String RECREATE_TABLE_IF_EXISTS = "dr";
    private byte[][] splitKeys;
    private NumberFormat numberFormat;


    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        new TestHbaseBlukLoad().cmdLineBegin(args);
    }

    private void cmdLineBegin(String[] args) throws InterruptedException, IOException {
        Options options = new Options();

        options.addOption(Option.builder(HELP)
                .longOpt("help")
                .desc("show this help message and exit program")
                .build());

        options.addOption(Option.builder(HFILE_DIR)
                .longOpt("hfiledir")
                .hasArg()
                .argName("hfiledir")
                .required(true)
                .desc("The root directory where the generated hfiles are placed" +
                        "\ne.g /test2020/hfiles")
                .build());

        options.addOption(Option.builder(CF_NAME)
                .longOpt("cfname")
                .hasArg()
                .argName("cfname")
                .required(true)
                .desc("The column family name" +
                        "\ne.g cf ")
                .build());

        options.addOption(Option.builder(HFILE_NAME)
                .longOpt("hfilename")
                .hasArg()
                .argName("hfilename")
                .required(true)
                .desc("The generated name")
                .build());

        options.addOption(Option.builder(TABLE_NAME)
                .longOpt("tablename")
                .hasArg()
                .argName("tablename")
                .required(true)
                .desc("New table name to load data into hbase")
                .build());

        options.addOption(Option.builder(ROWS_NUMBER)
                .longOpt("rowsnum")
                .hasArg()
                .argName("rowsnum")
                .required(true)
                .desc("How many rows of data to generate" +
                        "\ne.g 100")
                .build());

        options.addOption(Option.builder(ROW_SIZE)
                .longOpt("rowsize")
                .hasArg()
                .argName("rowsize")
                .required(true)
                .desc("Data size of each row" +
                        "\ne.g 20")
                .build());

        options.addOption(Option.builder(SPLITS)
                .longOpt("splits")
                .hasArg()
                .argName("splitsNum")
                .required(true)
                .desc("Number of regions to split the table into" +
                        "\ne.g 5")
                .build());

        options.addOption(Option.builder(REGION_SPLIT_COUNT)
                .longOpt("regionsplitcount")
                .hasArg()
                .argName("regionsplitcount")
                .required(true)
                .desc("Data size of each region" +
                        "\ne.g /root/conf/")
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

        options.addOption(Option.builder(RECREATE_TABLE_IF_EXISTS)
                .longOpt("drop")
                .desc("If the target table exists, drop it first")
                .build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine result = null;

        try {
            result = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("TestHbaseBlukLoad", options, true);
            System.exit(1);
        }

        if (result.hasOption(HELP)) {
            formatter.printHelp("TestHbaseBlukLoad", options, true);
            System.exit(0);
        }

        kerberosSwitch(result);
    }

    private void kerberosSwitch(CommandLine result) throws IOException {
        System.out.println("HFILE_DIR=" + result.getOptionValue(HFILE_DIR) +
                "\nCF_NAME=" + result.getOptionValue(CF_NAME) +
                "\nHFILE_NAME=" + result.getOptionValue(HFILE_NAME) +
                "\nTABLE_NAME=" + result.getOptionValue(TABLE_NAME) +
                "\nROWS_NUMBER=" + result.getOptionValue(ROWS_NUMBER) +
                "\nROW_SIZE=" + result.getOptionValue(ROW_SIZE) +
                "\nSPLITS=" + result.getOptionValue(SPLITS) +
                "\nREGION_SPLIT_COUNT=" + result.getOptionValue(REGION_SPLIT_COUNT) +
                "\nCONFIG_PATH=" + result.getOptionValue(CONFIG_PATH));

        ConfProperties.setPath(result.getOptionValue("c"));
        Configuration conf = initialConfiguration();

        if (result.hasOption(KERBEROS_ENABLE)) {
            System.out.println("kerberos is enabled!");
            System.setProperty("java.security.krb5.conf",
                    ConfProperties.getConf().getProperty("java.security.krb5.conf"));
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hbase.security.authentication", "kerberos");
            conf.set("hbase.master.kerberos.principal",
                    ConfProperties.getConf().getProperty("hbase.master.kerberos.principal"));
            conf.set("hbase.regionserver.kerberos.principal",
                    ConfProperties.getConf().getProperty("hbase.regionserver.kerberos.principal"));

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(ConfProperties.getConf().getProperty("krb.user.principle"),
                    ConfProperties.getConf().getProperty("krb.user.keytab"));


        } else {
            System.out.println("kerberos is disabled!");
            System.setProperty("HADOOP_USER_NAME", ConfProperties.getConf().getProperty("hadoop.user.name"));
        }

        Connection conn = ConnectionFactory.createConnection(conf);
        FileSystem fs = FileSystem.get(conf);
        Admin admin = conn.getAdmin();
        try {
            System.out.println("Start");
            /** create table */
            TableName tableName = createTable(result, admin);
            /** generate hfile */
            generateHfile(result, conf, fs);
            /** load hfile */
            loadHfileToHbase(result, conf, conn, admin, tableName);
        } catch (Exception e) {
            System.out.println("Exception");
            e.printStackTrace();
            System.out.println(e.getStackTrace());
            System.out.println(e.getCause());
            System.out.println(e.getMessage());
        } finally {
            fs.close();
            conn.close();
            admin.close();
        }

        System.out.println("End");
    }

    private Configuration initialConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        Set<String> strings = ConfProperties.getConf().stringPropertyNames();
        for (String propertyName : strings) {
            conf.set(propertyName, ConfProperties.getConf().getProperty(propertyName));
        }
        return conf;
    }

    private TableName createTable(CommandLine result, Admin admin) throws IOException {
        TableName tableName = TableName.valueOf(result.getOptionValue(TABLE_NAME));
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(
                Bytes.toBytes(result.getOptionValue(CF_NAME)));
        ColumnFamilyDescriptor cfd = cdb.build();
        tdb.setColumnFamily(cfd);
        TableDescriptor td = tdb.build();
        int splits = Integer.parseInt(result.getOptionValue(SPLITS));
        // byte[][] splitKeys = RegionSplitter.newSplitAlgoInstance(conf, "DecimalStringSplit").split(splits);
        byte[][] splitKeys = generateSplitKeys(Integer.parseInt(result.getOptionValue(ROWS_NUMBER)), splits);
        System.out.println("end create splitKeys");
        if (admin.tableExists(tableName)) {
            if (result.hasOption(RECREATE_TABLE_IF_EXISTS)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("delete exist table");
                admin.createTable(td, splitKeys);
            }
        } else {
            admin.createTable(td, splitKeys);
        }
        System.out.println("end create table");

        return tableName;
    }

    private byte[][] generateSplitKeys(int rows, int splits) {
        numberFormat = NumberFormat.getInstance();
        numberFormat.setGroupingUsed(false);
        int digitLength = (rows + "").length();
        numberFormat.setMaximumIntegerDigits(digitLength);
        numberFormat.setMinimumIntegerDigits(digitLength);
        int start = 0;
        int end = rows;
        int interval = (end - start) / splits;
        splitKeys = new byte[splits][];
        int index = 0;
        for (int i = interval; i <= end; i += interval) {
            System.out.println(index + ":" + new String(Bytes.toBytes(numberFormat.format(i))));
            splitKeys[index++] = Bytes.toBytes(numberFormat.format(i));
        }
        return splitKeys;
    }

    private void generateHfile(CommandLine result, Configuration conf, FileSystem fs) throws IOException {
        HFileContext hfileContext = new HFileContextBuilder().withCompression(Compression.Algorithm.NONE).build();
        Path hfilePathWithName = new Path(result.getOptionValue(HFILE_DIR) +
                File.separator + result.getOptionValue(CF_NAME) +
                File.separator + result.getOptionValue(HFILE_NAME));
        HFile.Writer writer = HFile.getWriterFactory(conf,
                new CacheConfig(conf)).withPath(fs, hfilePathWithName).withFileContext(hfileContext).create();

        ArrayList<List<String>> rowKeyLists = getRowKeyList(result.getOptionValue(ROWS_NUMBER));
        int count = 0;
        int fileCount = 0;
        int flag = Integer.parseInt(result.getOptionValue(REGION_SPLIT_COUNT));
        for (List<String> rowKeyList : rowKeyLists) {
            for (String s : rowKeyList) {
                if (count > flag) {
                    writer.close();
                    hfilePathWithName = new Path(result.getOptionValue(HFILE_DIR) +
                            File.separator + result.getOptionValue(CF_NAME) +
                            File.separator + result.getOptionValue(HFILE_NAME) +
                            fileCount);

                    writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, hfilePathWithName)
                            .withFileContext(hfileContext).create();
                    count = 0;
                    fileCount++;
                }

                KeyValue keyValue = new KeyValue(Bytes.toBytes(s), Bytes.toBytes(result.getOptionValue(CF_NAME)),
                        Bytes.toBytes("col1"),
                        Bytes.toBytes(RandomStringUtils.random(Integer.parseInt(result.getOptionValue(ROW_SIZE)))));
                writer.append(keyValue);
                count++;
            }
        }
        writer.close();
        fs.close();
        System.out.println("end generate hfile");
    }

    private void loadHfileToHbase(CommandLine result, Configuration conf, Connection conn, Admin admin, TableName tableName) throws IOException {
        Table table = conn.getTable(tableName);
        RegionLocator locator = conn.getRegionLocator(tableName);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        Path path = new Path(ConfProperties.getConf().getProperty("fs.defaultFS") + result.getOptionValue(HFILE_DIR));
        loader.doBulkLoad(path, admin, table, locator);
        System.out.println("end load hfile");
    }

    private ArrayList<List<String>> getRowKeyList(String rowsNum) {
        ArrayList<List<String>> list = new ArrayList<>();

        int left = 0;
        for (byte[] splitKeyByteArr : splitKeys) {
            int right = Integer.parseInt(new String(splitKeyByteArr));
            ArrayList<String> strings = new ArrayList<>();
            for (int i = left; i < right; i++) {
                strings.add(numberFormat.format(i));
            }
            left = right;
            Collections.sort(strings);
            list.add(strings);
        }
        // sort
        return list;
    }


}
