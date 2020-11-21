package com.hadoop.hbase.config;

import com.hadoop.hbase.fixmeta.ConfProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class ConfigHelper {
    public static Configuration getHadoopConf(boolean enableKrb) {
        System.setProperty("HADOOP_USER_NAME",ConfProperties.getConf().getProperty("hadoop.user.name"));
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", ConfProperties.getConf().get("fs.defaultFS").toString());
        conf.set("hbase.zookeeper.quorum", ConfProperties.getConf().get("hbase.zookeeper.quorum").toString());
        conf.set("hbase.zookeeper.property.clientPort",
                ConfProperties.getConf().get("hbase.zookeeper.property.clientPort").toString());
        conf.set("zookeeper.znode.parent", ConfProperties.getConf().get("zookeeper.znode.parent").toString());
        if(enableKrb){
            System.out.println("kerberos is enabled");
            System.setProperty("java.security.krb5.conf",
                    ConfProperties.getConf().get("java.security.krb5.conf").toString());
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hbase.security.authentication","kerberos");
            conf.set("hbase.master.kerberos.principal",ConfProperties.getConf().getProperty("hbase.master.kerberos.principal"));
            conf.set("hbase.regionserver.kerberos.principal",ConfProperties.getConf().getProperty("hbase.regionserver.kerberos.principal"));
        }
        return conf;
    }

    public static FileSystem getFileSystem(Configuration conf, boolean enableKrb) throws IOException, InterruptedException {
        FileSystem fs;
        if(enableKrb){
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                    ConfProperties.getConf().get("krb.user.principle").toString(),
                    ConfProperties.getConf().get("krb.user.keytab").toString());

            fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws IOException {
                    return FileSystem.get(conf);
                }
            });
        }else {
            fs = FileSystem.get(conf);
        }
        return fs;
    }
}
