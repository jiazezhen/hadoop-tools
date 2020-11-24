package com.hadoop.hbase.fixhfile;


import com.hadoop.hbase.config.ConfigHelper;
import com.hadoop.hbase.fixmeta.ConfProperties;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.util.*;

/**
 * @author jiazz
 * @version 1.0
 */
public class FixHFileTool {
    /**
     * The size of a (key length, value length) tuple that prefixes each entry in
     * a data block.
     */
    public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;
    // Previous blocks that were used in the course of the read
    private final ArrayList<HFileBlock> prevBlocks = new ArrayList<>();
    private ByteBuff blockBuffer;
    private   HFile.Reader reader;
    private int currTagsLen;
    private int currKeyLen;
    private int currValueLen;
    private long currMemstoreTS;
    private int currMemstoreTSLen;
    HFileBlock curBlock;


    private static final String HELP = "h";
    private static final String CONFIG_PATH = "c";
    private static final String KERBEROS_ENABLE = "k";
    private static final String CORRUPTED_HFILE_PATH = "e";
    private static final String FIXED_HFILE_PATH = "f";
    private static final String BLOCK_COUNT_P = "b";
    private static final String HEADER_SIZE_P = "d";
    private static final String MAX_DATA_SIZE_P = "d";
    private static final String COMPRESSION_ALGORITHM_P = "a";


    /**
     * If has checksum 33 else 24
     */
    private static int HEADER_SIZE;
    /**
     * How many data blocks does a hfile have
     * blockOffsets arraySize
     * e.g
     * 77M  4000
     * 223M 15000
     */
    private static int BLOCK_COUNT = 200000;

    /**
     * A hfile contains many data blocks,the default blockSize is 64k,
     * if we has compression the OnDiskSizeWithHeader will little than 64k,
     * found use snappy,the size is little than 32k,
     * we use snappy,so give the MAX_DATA_SIZE=32*1024
     */
    private static  int MAX_DATA_SIZE = 32768;

    private static Compression.Algorithm COMPRESSION_ALGORITHM;

    private static FileSystem fs;
    private static Configuration conf;
    
    public static void main(String[] args) throws IOException, InterruptedException {
        cmdLineBegin(args);

    }
    private static void cmdLineBegin(String[] args) throws IOException, InterruptedException {
        Options options = new Options();

        options.addOption(Option.builder(HELP)
                .longOpt("help")
                .desc("show this help message and exit program")
                .build());


        options.addOption(Option.builder(CONFIG_PATH)
                .longOpt("config")
                .hasArg()
                .argName("configPath")
                .required(true)
                .desc("Directory to configuration.properties" +
                        "\ne.g /root/conf/")
                .build());

        options.addOption(Option.builder(CORRUPTED_HFILE_PATH)
                .longOpt("corruptHFilePath")
                .hasArg()
                .argName("corruptHFilePath")
                .required(true)
                .desc("Path to corrupt hfile path" +
                        "\ne.g /corruptHFile/deba1987a1a14c6ba371eacd7df0340b")
                .build());

        options.addOption(Option.builder(FIXED_HFILE_PATH)
                .longOpt("fixedHFilePath")
                .hasArg()
                .argName("fixedHFilePath")
                .required(true)
                .desc("Path to put fixed hfile path" +
                        "\ne.g /fixedHFile/deba1987a1a14c6ba371eacd7df0340b")
                .build());

        options.addOption(Option.builder(BLOCK_COUNT_P)
                .longOpt("blockCount")
                .hasArg()
                .argName("blockCount")
                .desc("Give a max blockCount, How many dataBlocks dose a hfile have" +
                        "\ne.g 70M hfile give 4000,223M give 15000")
                .build());

        options.addOption(Option.builder(HEADER_SIZE_P)
                .longOpt("headerSize")
                .hasArg()
                .argName("headerSize")
                .desc("BlockHeader Size,if has checksum 33,else 24")
                .build());

        options.addOption(Option.builder(MAX_DATA_SIZE_P)
                .longOpt("maxDataSize")
                .hasArg()
                .argName("maxDataSize")
                .desc("A hfile contains many data blocks,the default blockSize is 64k," +
                        "if we has compression the OnDiskSizeWithHeader will little than 64k," +
                        "usually if use snappy,the size is little than 32k,so default is 32k")
                .build());

        options.addOption(Option.builder(COMPRESSION_ALGORITHM_P)
                .longOpt("compressionAlgorithm")
                .hasArg()
                .argName("compressionAlgorithm")
                .desc("Compression Algorithm \nsupport values:none,snappy,lzo,gz,lz4,bzip2,zstd \n defalut: none")
                .build());


        options.addOption(Option.builder(KERBEROS_ENABLE)
                .longOpt("kerberos")
                .desc("If the cluster has Kerberos enabled,default is disable")
                .build());


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine result = null;

        try {
            result = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("FixHFileTool", options, true);
            System.exit(1);
        }

        if (result.hasOption(HELP)) {
            formatter.printHelp("FixHFileTool", options, true);
            System.exit(0);
        }

        if (result.hasOption(CORRUPTED_HFILE_PATH) && result.hasOption(FIXED_HFILE_PATH) && result.hasOption(CONFIG_PATH)) {
            System.out.println("corruptHFilePath=" + result.getOptionValue(CORRUPTED_HFILE_PATH)
                    + "\nfixedHFilePath=" + result.getOptionValue(FIXED_HFILE_PATH)
                    + "\nconfigpath=" + result.getOptionValue(CONFIG_PATH));

            HEADER_SIZE = Integer.parseInt(result.getOptionValue(HEADER_SIZE_P, "33"));
            BLOCK_COUNT = Integer.parseInt(result.getOptionValue(BLOCK_COUNT_P,"200000"));
            MAX_DATA_SIZE = Integer.parseInt(result.getOptionValue(MAX_DATA_SIZE_P,"32768"));

            String compressAlg = result.getOptionValue(COMPRESSION_ALGORITHM_P,"none");
            switch (compressAlg){
                case "snappy":
                    COMPRESSION_ALGORITHM = Compression.Algorithm.SNAPPY;
                    break;
                case "lzo":
                    COMPRESSION_ALGORITHM = Compression.Algorithm.LZO;
                    break;
                case "bzip2":
                    COMPRESSION_ALGORITHM = Compression.Algorithm.BZIP2;
                    break;
                case "lz4":
                    COMPRESSION_ALGORITHM = Compression.Algorithm.LZ4;
                    break;
                case "gz":
                    COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
                    break;
                case "zstd":
                    COMPRESSION_ALGORITHM = Compression.Algorithm.ZSTD;
                    break;
                default:
                    COMPRESSION_ALGORITHM = Compression.Algorithm.NONE;

            }


            kerberosSwitch(result);

        } else {
            formatter.printHelp("FixHFileTool", options, true);
        }
    }

    private static void kerberosSwitch(CommandLine result) throws IOException, InterruptedException {
        ConfProperties.setPath(result.getOptionValue("c"));
        conf = ConfigHelper.getHadoopConf(result.hasOption(KERBEROS_ENABLE));
        fs = ConfigHelper.getFileSystem(conf,result.hasOption(KERBEROS_ENABLE));
        Path corruptPath = new Path(result.getOptionValue(CORRUPTED_HFILE_PATH));
        Path fixedPath = new Path(result.getOptionValue(FIXED_HFILE_PATH));
        FixHFileTool fixHFileTool = new FixHFileTool();
        fixHFileTool.kvReaderAndWriter(corruptPath,fixedPath);
    }


    /**
     * Read the corruptHFile's normal data,And generate a newHFile
     *
     * @param corruptHFile HFile contains broken dataBlocks
     * @param fixedFile Fixed HFile
     * @throws IOException
     */
    public void kvReaderAndWriter (Path corruptHFile, Path fixedFile) throws IOException {
        long beginTime = System.currentTimeMillis();
        System.out.println("********************start fix hfile: "+ corruptHFile.toString()+"**************");
        reader = HFile.createReader(fs, corruptHFile, CacheConfig.DISABLED, true,fs.getConf());
        HFileContext hfileContext = new HFileContextBuilder().withCompression(COMPRESSION_ALGORITHM).build();

        HFile.Writer writer = HFile.getWriterFactory(conf,
                new CacheConfig(conf)).withPath(fs, fixedFile).withFileContext(hfileContext).create();
        long fileSize = fs.getFileStatus(corruptHFile).getLen();

        try {
            FSDataInputStreamWrapper fsdis  = new FSDataInputStreamWrapper(fs, corruptHFile);
            FixedFileTrailer trailer =
                    FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
            long max = trailer.getLastDataBlockOffset();
            // long offset = trailer.getFirstDataBlockOffset();
            HFileBlock block;

            int[] blockDataSizes = new int[BLOCK_COUNT];
            long[] blookOffsets = new long[BLOCK_COUNT];
            getAllBlockOffsetAndDataSize(corruptHFile,blookOffsets,blockDataSizes);
            long offset;
            int index = 0;
            int blockCount = 0;
            int kvCount=0;

            // read blocks
            while (blookOffsets[index] <= max && blockDataSizes[index] !=0) {
                offset = blookOffsets[index];
                index++;
                /**
                 * the broken blocks datasize always bigger than a nomal one,
                 * discass
                 */
                if(blockDataSizes[index-1]> MAX_DATA_SIZE)
                    continue;
    
                block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
                        /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);
                // offset += block.getOnDiskSizeWithHeader();
                blockCount++;
                blockBuffer =block.getBufferWithoutHeader();
                updateCurrBlockRef(block);
                System.out.println("<<<<<<<<<<<<<<<<<<<<writing block ... ... ... ... ...\n"+block);
                // read kv
                do{
                    readKeyValueLen();
                    if(this.currKeyLen == 0){
                        System.out.println("skip 0 datasize, offset = " + block.getOffset());
                        continue;
                    }
                    // System.out.println(block);
                    Cell cell = getCell();
                    System.out.println("write cell:\t" + cell);
                    writer.append(cell);
                    kvCount++;
                    blockBuffer.skip(getCurCellSerializedSize());
                }while (blockBuffer.remaining()>0);
            }
            System.out.println("trailer.getLastDataBlockOffset()="+trailer.getLastDataBlockOffset());
            System.out.println("***********************finish fix hfile: "+ corruptHFile.toString()+"\t--->newFile="+fixedFile.toString()+"***********");
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setMaximumFractionDigits(2);
            String lossRate = numberFormat.format(((float)trailer.getEntryCount()-(float)kvCount)/trailer.getEntryCount());
            System.out.println("OldFile KV=" + trailer.getEntryCount()+"\tNewFile KV="+kvCount + "\t lossRate="+lossRate);
            System.out.println("OldFile Size="+fileSize + "\tNewFile blockCount="+blockCount);
            long endTime = System.currentTimeMillis();
            System.out.println("totalUsedTime: "+(endTime-beginTime)/1000+"s");
        } finally {
            writer.close();
            fs.close();
        }

    }

    /**
     * Compute blocks offset and dataSize(onDiskSizeWithHeader)
     *
     * if had blockType magicStr broken, the computed dataSize maybe two or more blocks sumSize
     * @param file hFile path
     * @param blockOffsets  blockOffset array
     * @param blockDataSizes blockDataSize array
     * @throws IOException
     */
    public void getAllBlockOffsetAndDataSize(Path file, long[] blockOffsets, int[] blockDataSizes) throws IOException {
        System.out.println("-------------start compute hFile block's offset and dataSize -----------------");
        FSDataInputStream fsDataInputStream = fs.open(file);
        long fileSize = fs.getFileStatus(file).getLen();
        System.out.println("fileSize="+fileSize);
        int count = 0;
        int offset = 0;
        long blockOffset = -1;
        byte[] oneByte = new byte[1];
        // fsDataInputStream.seek(26684325);

        byte[] windowBytes = new byte[8];
        while (fsDataInputStream.read(oneByte)!=-1 ){
            offset +=1;
            ByteArrayHelper.removeFirstAndAppend(windowBytes,oneByte[0]);
            /**
             * Scanned Block Section contains 3 types block: DATA、BLOOM_CHUNK、LEAF_INDEX
             * not sure if has to compute BLOOM_CHUNK and LEAF_INDEX
             * I think the newFile will loss some blocks, the bloom blocks and leaf index block will change too
             * if needed ,replace as follows
             */
            // boolean isDataBlock = Arrays.equals(windowBytes, "DATABLK*".getBytes())
            //         || Arrays.equals(windowBytes,"BLMFBLK2".getBytes())
            //         || Arrays.equals(windowBytes,"IDXLEAF2".getBytes());
            boolean isDataBlock = Arrays.equals(windowBytes, "DATABLK*".getBytes());
            if(isDataBlock){
                if(count>0){
                    blockOffset = offset-8;
                    blockOffsets[count] = blockOffset;
                    //currentBlockOffset - preBlockOffset = datasize
                    blockDataSizes[count-1] = (int)(blockOffset - blockOffsets[count-1]);
                }else {
                    blockOffset = 0;
                    blockOffsets[count] = 0;
                }
                System.out.println("found the " + count + "th block,\toffset="+blockOffset);
                count++;
            }
        }
        int lastDataSize = BlockHeaderTools.getOnDiskSizeWithoutHeader(fsDataInputStream, blockOffset)+ HEADER_SIZE;
        blockDataSizes[count-1] = lastDataSize;
        fsDataInputStream.close();
        System.out.println("-------------finish compute hFile block's offset and dataSize -----------------");
    }



    // Returns the #bytes in HFile for the current cell. Used to skip these many bytes in current
    // HFile block's buffer so as to position to the next cell.
    private int getCurCellSerializedSize() {
        int curCellSize =  KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen
                + currMemstoreTSLen;
        if (this.reader.getFileContext().isIncludesTags()) {
            curCellSize += Bytes.SIZEOF_SHORT + currTagsLen;
        }
        return curCellSize;
    }


    private int getKVBufSize() {
        int kvBufSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
        if (currTagsLen > 0) {
            kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
        }
        return kvBufSize;
    }

    public Cell getCell() {

       int  currTagsLen = 0;
        Cell ret;
        int cellBufSize = getKVBufSize();
        long seqId = 0L;
        if (this.reader.shouldIncludeMemStoreTS()) {
            seqId = currMemstoreTS;
        }
        if (blockBuffer.hasArray()) {
            // TODO : reduce the varieties of KV here. Check if based on a boolean
            // we can handle the 'no tags' case.
            if (currTagsLen > 0) {
                ret = new SizeCachedKeyValue(blockBuffer.array(),
                        blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId);
            } else {
                ret = new SizeCachedNoTagsKeyValue(blockBuffer.array(),
                        blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId);
            }
        } else {
            ByteBuffer buf = blockBuffer.asSubByteBuffer(cellBufSize);
            if (buf.isDirect()) {
                ret = currTagsLen > 0 ? new ByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId)
                        : new NoTagsByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId);
            } else {
                if (currTagsLen > 0) {
                    ret = new SizeCachedKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
                            cellBufSize, seqId);
                } else {
                    ret = new SizeCachedNoTagsKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
                            cellBufSize, seqId);
                }
            }
        }
        return ret;
    }



    void updateCurrBlockRef(HFileBlock block) {
        if (block != null && this.curBlock != null &&
                block.getOffset() == this.curBlock.getOffset()) {
            return;
        }
        // We don't have to keep ref to EXCLUSIVE type of block
        if (this.curBlock != null && this.curBlock.usesSharedMemory()) {
            prevBlocks.add(this.curBlock);
        }
        this.curBlock = block;
    }

    protected void readKeyValueLen() {
        // This is a hot method. We go out of our way to make this method short so it can be
        // inlined and is not too big to compile. We also manage position in ByteBuffer ourselves
        // because it is faster than going via range-checked ByteBuffer methods or going through a
        // byte buffer array a byte at a time.
        // Get a long at a time rather than read two individual ints. In micro-benchmarking, even
        // with the extra bit-fiddling, this is order-of-magnitude faster than getting two ints.
        // Trying to imitate what was done - need to profile if this is better or
        // earlier way is better by doing mark and reset?
        // But ensure that you read long instead of two ints
        long ll = blockBuffer.getLongAfterPosition(0);
        // Read top half as an int of key length and bottom int as value length
        this.currKeyLen = (int)(ll >> Integer.SIZE);
        this.currValueLen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
        checkKeyValueLen();
        // Move position past the key and value lengths and then beyond the key and value
        int p = (Bytes.SIZEOF_LONG + currKeyLen + currValueLen);
        if (reader.getFileContext().isIncludesTags()) {
            // Tags length is a short.
            this.currTagsLen = blockBuffer.getShortAfterPosition(p);
            checkTagsLen();
            p += (Bytes.SIZEOF_SHORT + currTagsLen);
        }
        readMvccVersion(p);
    }

    /**
     * Read mvcc. Does checks to see if we even need to read the mvcc at all.
     * @param offsetFromPos
     */
    protected void readMvccVersion(final int offsetFromPos) {
        // See if we even need to decode mvcc.
        if (!this.reader.shouldIncludeMemStoreTS()) return;
        if (!this.reader.isDecodeMemStoreTS()) {
            currMemstoreTS = 0;
            currMemstoreTSLen = 1;
            return;
        }
        _readMvccVersion(offsetFromPos);
    }

    /**
     * Actually do the mvcc read. Does no checks.
     * @param offsetFromPos
     */
    private void _readMvccVersion(int offsetFromPos) {
        // This is Bytes#bytesToVint inlined so can save a few instructions in this hot method; i.e.
        // previous if one-byte vint, we'd redo the vint call to find int size.
        // Also the method is kept small so can be inlined.
        byte firstByte = blockBuffer.getByteAfterPosition(offsetFromPos);
        int len = WritableUtils.decodeVIntSize(firstByte);
        if (len == 1) {
            this.currMemstoreTS = firstByte;
        } else {
            int remaining = len -1;
            long i = 0;
            offsetFromPos++;
            if (remaining >= Bytes.SIZEOF_INT) {
                // The int read has to be converted to unsigned long so the & op
                i = (blockBuffer.getIntAfterPosition(offsetFromPos) & 0x00000000ffffffffL);
                remaining -= Bytes.SIZEOF_INT;
                offsetFromPos += Bytes.SIZEOF_INT;
            }
            if (remaining >= Bytes.SIZEOF_SHORT) {
                short s = blockBuffer.getShortAfterPosition(offsetFromPos);
                i = i << 16;
                i = i | (s & 0xFFFF);
                remaining -= Bytes.SIZEOF_SHORT;
                offsetFromPos += Bytes.SIZEOF_SHORT;
            }
            for (int idx = 0; idx < remaining; idx++) {
                byte b = blockBuffer.getByteAfterPosition(offsetFromPos + idx);
                i = i << 8;
                i = i | (b & 0xFF);
            }
            currMemstoreTS = (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
        }
        this.currMemstoreTSLen = len;
    }


    private final void checkTagsLen() {
        if (checkLen(this.currTagsLen)) {
            throw new IllegalStateException("Invalid currTagsLen " + this.currTagsLen +
                    ". Block offset: " + curBlock.getOffset() + ", block length: " +
                    this.blockBuffer.limit() +
                    ", position: " + this.blockBuffer.position() + " (without header)." +
                    " path=" + reader.getPath());
        }
    }

    /**
     * @param v
     * @return True if v &lt; 0 or v &gt; current block buffer limit.
     */
    protected final boolean checkLen(final int v) {
        return v < 0 || v > this.blockBuffer.limit();
    }

    /**
     * Check key and value lengths are wholesome.
     */
    protected final void checkKeyValueLen() {
        if (checkKeyLen(this.currKeyLen) || checkLen(this.currValueLen)) {
            throw new IllegalStateException("Invalid currKeyLen " + this.currKeyLen
                    + " or currValueLen " + this.currValueLen + ". Block offset: "
                    + this.curBlock.getOffset() + ", block length: "
                    + this.blockBuffer.limit() + ", position: " + this.blockBuffer.position()
                    + " (without header)." + ", path=" + reader.getPath());
        }
    }

    /**
     * @param v
     * @return True if v &lt;= 0 or v &gt; current block buffer limit.
     */
    protected final boolean checkKeyLen(final int v) {
        return v <= 0 || v > this.blockBuffer.limit();
    }

}
