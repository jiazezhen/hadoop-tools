### hadoop-tools

This packge provided 2 tools:

1. Following steps will build out the jars

```
$ git clone https://github.com/zhaoyim/hadoop-tools.git
$ mvn install
```

_NOTE: The TestHbaseBulkLoad-1.0.jar and the build out ```lib``` folder should in the same folder, not matter you want to copy the jar into any path._


2.  FixHbaseMeta-1.0.jar is used to rebuild the Hbase meta data from HDFS

	1) Then run ```java -jar FixHbaseMeta-1.0.jarr --help
``` you can get the help info


	```
	$java -jar FixHbaseMeta-1.0.jar
	Missing required options: d, n, t, c
	usage: FixHbaseMeta -c <configpath> -d <datapath> [-h] [-k] -n <namespace>
	       -t <tablename>
	 -c,--config <configpath>     Path to configuration.properties
	                              e.g /root/conf/
	 -d,--data <datapath>         The metadata path of HBase on HDFS
	                              e.g /apps/hbase/data/data/
	 -h,--help                    show this help message and exit program
	 -k,--kerberos                If the cluster has Kerberos enabled, this is
	                              required
	 -n,--namespace <namespace>   The namespace of table
	                              e.g default hbase
	 -t,--tablename <tablename>   Which table's metadata to fix
	```

	2) configuration.properties file should configure following message

	```
	### For FixHbaseMeta
	# hadoop user eg: ocdp
	hadoop.user.name=hdfs
	hbase.zookeeper.quorum=<zk hostname or ip>
	hbase.zookeeper.property.clientPort=<zk port>
	# hbase zk node eg: /hbase-unsecure
	zookeeper.znode.parent=<hbase zk node>
	# default fs eg: hdfs://host-10-1-236-60:8020
	fs.defaultFS=<default fs>
	# if enbaled krb you should configure the krb related message
	java.security.krb5.conf=/Users/zezhenjia/Downloads/hadoop-tools/krb5.conf
	krb.user.principle=hbase-mycluster@OCDP.COM
	krb.user.keytab=/Users/zezhenjia/Downloads/hadoop-tools/hbase.headless.keytab
	hbase.master.kerberos.principal=hbase/_HOST@OCDP.COM
	hbase.regionserver.kerberos.principal=hbase/_HOST@OCDP.COM
	```

3.  TestHbaseBulkLoad-1.0.jar is used to test generate Hfile by HFile.Writer then use LoadIncrementalHFiles to load the data into Hbase


	1) Then run ```java -jar TestHbaseBulkLoad-1.0.jar --help
``` you can get the help info


	```
	$ java -jar TestHbaseBulkLoad-1.0.jar --help
	Missing required options: d, cf, fn, t, rn, rs, s, rsc, c
	usage: TestHbaseBlukLoad -c <configpath> -cf <cfname> -d <hfiledir> -fn
	       <hfilename> [-h] [-k] -rn <rowsnum> -rs <rowsize> -rsc
	       <regionsplitcount> -s <splitsNum> -t <tablename>
	 -c,--config <configpath>                     Path to
	                                              configuration.properties
	                                              e.g /root/conf/
	 -cf,--cfname <cfname>                        The column family name
	                                              e.g cf
	 -d,--hfiledir <hfiledir>                     The root directory where the
	                                              generated hfiles are placed
	                                              e.g /test2020/hfiles
	 -fn,--hfilename <hfilename>                  The generated name
	 -h,--help                                    show this help message and
	                                              exit program
	 -k,--kerberos                                If the cluster has Kerberos
	                                              enabled, this is required
	 -rn,--rowsnum <rowsnum>                      How many rows of data to
	                                              generate
	                                              e.g 100
	 -rs,--rowsize <rowsize>                      Data size of each row
	                                              e.g 20
	 -rsc,--regionsplitcount <regionsplitcount>   Data size of each region
	                                              e.g /root/conf/
	 -s,--splits <splitsNum>                      Number of regions to split
	                                              the table into
	                                              e.g 5
	 -t,--tablename <tablename>                   New table name to load data
	                                              into hbase
	```

	2) configuration.properties file should configure following message

	```
	### For TestHbaseBulkLoad
	# hadoop user eg: ocdp
	hadoop.user.name=<hadoop user>
	hbase.zookeeper.quorum=<zk hostname or ip>
	hbase.zookeeper.property.clientPort=<zk port>
	# hbase zk node eg: /hbase-secure
	zookeeper.znode.parent=<hbase zk node>
	# default fs eg: hdfs://host-10-1-236-60:8020
	fs.defaultFS=<default fs>
	hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024
	# if enbaled krb you should configure the krb related message
	hbase.master.kerberos.principal=hbase/_HOST@OCDP.COM
	hbase.regionserver.kerberos.principal=hbase/_HOST@OCDP.COM
	java.security.krb5.conf=/Users/zezhenjia/Downloads/hadoop-tools/krb5.conf
	krb.user.principle=ocdp-mycluster@OCDP.COM
	krb.user.keytab=/Users/zezhenjia/Downloads/hadoop-tools/hbase.headless.keytab
	```


