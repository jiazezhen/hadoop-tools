package com.hadoop.hbase.fixhfile;

import com.hadoop.hbase.config.ConfigHelper;
import com.hadoop.hbase.fixmeta.ConfProperties;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 调优，一边儿计算offset datasize一边儿读block写cell
 * 发现更慢了，暂时放一下
 */
public class FixHFileFinalTool2 {
    private static Logger logger = Logger.getLogger(FixHFileFinalTool.class);

    private static final String HELP = "h";
    private static final String CONFIG_PATH = "c";
    private static final String KERBEROS_ENABLE = "k";
    private static final String CORRUPTED_HFILE_PATH = "e";
    private static final String FIXED_HFILE_PATH = "f";
    private static final String BLOCK_COUNT_P = "b";
    private static final String HEADER_SIZE_P = "i";
    private static final String MAX_DATA_SIZE_P = "d";
    private static final String COMPRESSION_ALGORITHM_P = "a";
    private static final String DISCARD_LAST_BLOCK_P = "l";

    /**
     * A hfile contains many data blocks,the default blockSize is 64k,
     * if we has compression the OnDiskSizeWithHeader will little than 64k,
     * found use snappy,the size is little than 32k,
     * we use snappy,so give the MAX_DATA_SIZE=32*1024
     */
    private static  int MAX_DATA_SIZE = 32768;

    /** If has checksum 33 else 24 **/
    private static int HEADER_SIZE = 33;
    private static int BLOCK_COUNT = 200000;
    private static boolean DISCARD_LAST_BLOCK = false;

    private static Compression.Algorithm COMPRESSION_ALGORITHM = Compression.Algorithm.SNAPPY;


    private ByteBuff blockBuffer;
    private HFileBlock curBlock;
    private Path filePath;
    /**corrupt hFile path*/
    private int currTagsLen;
    private int currKeyLen;
    private int currValueLen;
    private long currMemstoreTS;
    private int currMemstoreTSLen;

    /**Whether tags are to be included in the Read/Write**/
    private boolean includesTags = false;
    private boolean includesMemstoreTS = true;
    protected boolean decodeMemstoreTS = true;

    private static final int DATA_IBUF_SIZE = 1 * 1024;

    /**
     * The size of a (key length, value length) tuple that prefixes each entry in
     * a data block.
     */
    public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;


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
                .desc("BlockHeader Size,if has checksum 33,else 24\n default:33")
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
                .desc("Compression Algorithm \nsupport values:none,snappy,lzo,gz,lz4,bzip2,zstd \n default: none")
                .build());


        options.addOption(Option.builder(KERBEROS_ENABLE)
                .longOpt("kerberos")
                .desc("If the cluster has Kerberos enabled,default is disable")
                .build());

        options.addOption(Option.builder(DISCARD_LAST_BLOCK_P)
                .longOpt("discardLastBlock")
                .desc("found if the trailer was damaged, the last block always cannot be decompressed\n default: false")
                .build());


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine result = null;

        try {
            result = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("FixHFileFinalTool", options, true);
            System.exit(1);
        }

        if (result.hasOption(HELP)) {
            formatter.printHelp("FixHFileFinalTool", options, true);
            System.exit(0);
        }

        if (result.hasOption(CORRUPTED_HFILE_PATH) && result.hasOption(FIXED_HFILE_PATH) && result.hasOption(CONFIG_PATH)) {
            logger.info("corruptHFilePath=" + result.getOptionValue(CORRUPTED_HFILE_PATH)
                    + "\nfixedHFilePath=" + result.getOptionValue(FIXED_HFILE_PATH)
                    + "\nconfigPath=" + result.getOptionValue(CONFIG_PATH));

            HEADER_SIZE = Integer.parseInt(result.getOptionValue(HEADER_SIZE_P, "33"));
            BLOCK_COUNT = Integer.parseInt(result.getOptionValue(BLOCK_COUNT_P,"200000"));
            MAX_DATA_SIZE = Integer.parseInt(result.getOptionValue(MAX_DATA_SIZE_P,"32768"));
            initCompressionAlgorithm(result.getOptionValue(COMPRESSION_ALGORITHM_P,"none"));
            if(result.hasOption(DISCARD_LAST_BLOCK_P)){
                DISCARD_LAST_BLOCK=true;
            }

            kerberosSwitch(result);

        } else {
            formatter.printHelp("FixHFileTool", options, true);
        }
    }

    private static void initCompressionAlgorithm(String compressAlg ){
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
    }

    private static void kerberosSwitch(CommandLine result) throws IOException, InterruptedException {
        ConfProperties.setPath(result.getOptionValue("c"));
        conf = ConfigHelper.getHadoopConf(result.hasOption(KERBEROS_ENABLE));
        fs = ConfigHelper.getFileSystem(conf,result.hasOption(KERBEROS_ENABLE));
        Path corruptPath = new Path(result.getOptionValue(CORRUPTED_HFILE_PATH));
        Path fixedPath = new Path(result.getOptionValue(FIXED_HFILE_PATH));
        FixHFileFinalTool fixHFileFinalTool = new FixHFileFinalTool();
        fixHFileFinalTool.ioReaderAndWriter(corruptPath, fixedPath);
    }

    public void ioReaderAndWriter2(Path corruptHFile, Path fixedFile) throws IOException {
        long beginTime = System.currentTimeMillis();
        logger.info("********************start fix hfile: "+ corruptHFile.toString()+"**************");
        HFileContext hfileContext = new HFileContextBuilder().withCompression(COMPRESSION_ALGORITHM).build();
        HFile.Writer writer = HFile.getWriterFactory(conf,
                new CacheConfig(conf)).withPath(fs, fixedFile).withFileContext(hfileContext).create();
        FSDataInputStream inputStream = fs.open(corruptHFile);
        filePath = corruptHFile;
        byte[] oneByte = new byte[1];
        long offset = 0;
        byte[] windowBytes = new byte[8];
        long preBlockOffset = -1;
        long currentBlockOffset = 0;
        int blockDataSize = 0;
        while (inputStream.read(oneByte)!=-1 ){
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
                if(offset > HEADER_SIZE){
                    preBlockOffset = currentBlockOffset;
                    currentBlockOffset = offset - 8;
                    blockDataSize = (int)(currentBlockOffset - preBlockOffset);
                    inputStream.seek(preBlockOffset);
                    readBlockAndWriteToFile(writer,inputStream,preBlockOffset,blockDataSize);
                    inputStream.seek(offset);
                }else {
                    currentBlockOffset = 0;
                }
            }
        }
        /**read and write last block**/
        inputStream.seek(currentBlockOffset);
        blockDataSize = BlockHeaderTools.getOnDiskSizeWithoutHeader(inputStream, currentBlockOffset)+ HEADER_SIZE;
        readBlockAndWriteToFile(writer,inputStream,currentBlockOffset,blockDataSize);
        writer.close();
        inputStream.close();

        long endTime = System.currentTimeMillis();
        logger.info("=======totalUsedTime: "+(endTime-beginTime)/1000+"s======");

    }

    private void readBlockAndWriteToFile(HFile.Writer writer, FSDataInputStream inputStream, long offset, int onDiskSizeWithHeader) throws IOException {

        int onDiskSizeWithoutHeader = onDiskSizeWithHeader - HEADER_SIZE;
        byte[] headerBytes = new byte[HEADER_SIZE];
        inputStream.readFully(offset, headerBytes, 0, HEADER_SIZE);
        byte[] onDiskBlockWithoutHeaderBytes = new byte[onDiskSizeWithoutHeader];
        inputStream.readFully(offset + HEADER_SIZE, onDiskBlockWithoutHeaderBytes, 0, onDiskSizeWithoutHeader);
        byte[] onDiskBlock = new byte[onDiskSizeWithHeader];
        System.arraycopy(headerBytes, 0, onDiskBlock, 0, HEADER_SIZE);
        System.arraycopy(onDiskBlockWithoutHeaderBytes, 0, onDiskBlock, HEADER_SIZE, onDiskSizeWithoutHeader);
        ByteBuffer onDiskBlockByteBuffer = ByteBuffer.wrap(onDiskBlock, 0, onDiskSizeWithHeader);
        HFileContextBuilder hFileContextBuilder = new HFileContextBuilder();
        hFileContextBuilder.withCompression(COMPRESSION_ALGORITHM);
        HFileContext fileContext = hFileContextBuilder.build();
        HFileBlock blk =
                new HFileBlock(new SingleByteBuff(onDiskBlockByteBuffer), true,
                        Cacheable.MemoryType.EXCLUSIVE, offset, 0, fileContext);
        curBlock = blk;
        if (!COMPRESSION_ALGORITHM.equals(Compression.Algorithm.NONE)) {
            ByteBuff dup = blk.buf.duplicate();
            dup.position(blk.headerSize());
            dup = dup.slice();
            ByteBuffInputStream byteBuffInputStream = new ByteBuffInputStream(dup);
            InputStream dataInputStream = new DataInputStream(byteBuffInputStream);
            Decompressor decompressor = COMPRESSION_ALGORITHM.getDecompressor();
            CompressionCodec codec = COMPRESSION_ALGORITHM.getCodec();
            BlockDecompressorStream blockDecompressorStream = (BlockDecompressorStream) codec.createInputStream(dataInputStream, decompressor);
            InputStream inputStreamOfUncompress = new BufferedInputStream(blockDecompressorStream, DATA_IBUF_SIZE);
            try {
                int uncompressedSize = 0;
                inputStreamOfUncompress.mark(1);
                if (inputStreamOfUncompress.read() != -1) {
                    uncompressedSize = blockDecompressorStream.originalBlockSize;
                }
                inputStreamOfUncompress.reset();
                allocateBuffer(blk, uncompressedSize);
                IOUtils.readFully(inputStreamOfUncompress, blk.getBufferWithoutHeader().array(), blk.getBufferWithoutHeader().arrayOffset(), uncompressedSize);
            } catch (Error error) {
                logger.debug(error);
                return;
            } finally {
                inputStreamOfUncompress.close();
                blockDecompressorStream.close();
                byteBuffInputStream.close();
                dataInputStream.close();
            }
        } else {
            allocateBuffer(blk, onDiskSizeWithoutHeader);
            System.arraycopy(onDiskBlockWithoutHeaderBytes, 0, blk.getBufferWithoutHeader().array(), blk.getBufferWithoutHeader().arrayOffset(), onDiskSizeWithoutHeader);
        }

        blockBuffer = blk.getBufferWithoutHeader();

        logger.debug("............writing block............\n" + blk);
        do {
            try {
                readKeyValueLen();
                Cell cell = getCell();
                writer.append(cell);
                blockBuffer.skip(getCurCellSerializedSize());
            } catch (Exception e) {
                logger.debug(e);
                break;
            }
        } while (blockBuffer.remaining() > 0);

    }


    public void ioReaderAndWriter(Path corruptHFile, Path fixedFile) throws IOException {
        long beginTime = System.currentTimeMillis();
        logger.info("********************start fix hfile: "+ corruptHFile.toString()+"**************");
        HFileContext hfileContext = new HFileContextBuilder().withCompression(COMPRESSION_ALGORITHM).build();
        HFile.Writer writer = HFile.getWriterFactory(conf,
                new CacheConfig(conf)).withPath(fs, fixedFile).withFileContext(hfileContext).create();
        FSDataInputStream inputStream = fs.open(corruptHFile);
        filePath = corruptHFile;

        logger.info("fileSize="+fs.getFileStatus(corruptHFile).getLen());
        logger.info("**********getAllBlockOffsetAndDataSize**********");
        int[] blockDataSizes = new int[BLOCK_COUNT];
        long[] blockOffsets = new long[BLOCK_COUNT];
        getAllBlockOffsetAndDataSize(corruptHFile,blockOffsets,blockDataSizes);
        // blockOffsets[0] = 2073032;
        // blockDataSizes[0] = 20822;
        // blockOffsets[1] = 2093854;
        // blockDataSizes[1] = 20881;
        // blockOffsets[2] = -1;
        // blockDataSizes[2] = -1;


        logger.info("**********readBlock,decompressBlock and writeBlock**********");
        try {
            int index = 0;
            while (blockDataSizes[index] !=-1) {
                /**found if the trailer is damaged, the last block always cannot be decompressed**/
                // if(DISCARD_LAST_BLOCK && blockDataSizes[index+1] == -1){
                //     index++;
                //     continue;
                // }
                long offset = blockOffsets[index];
                int onDiskSizeWithHeader = blockDataSizes[index];
                int onDiskSizeWithoutHeader = onDiskSizeWithHeader - HEADER_SIZE;
                index++;

                // if(blockDataSizes[index-1]> MAX_DATA_SIZE)
                //     continue;

                byte[] headerBytes = new byte[HEADER_SIZE];
                inputStream.readFully(offset,headerBytes,0,HEADER_SIZE);
                byte[] onDiskBlockWithoutHeaderBytes = new byte[onDiskSizeWithoutHeader] ;
                inputStream.readFully(offset+HEADER_SIZE,onDiskBlockWithoutHeaderBytes,0,onDiskSizeWithoutHeader);
                byte [] onDiskBlock = new byte[onDiskSizeWithHeader];
                System.arraycopy(headerBytes,0,onDiskBlock,0,HEADER_SIZE);
                System.arraycopy(onDiskBlockWithoutHeaderBytes,0,onDiskBlock,HEADER_SIZE,onDiskSizeWithoutHeader);
                ByteBuffer onDiskBlockByteBuffer = ByteBuffer.wrap(onDiskBlock, 0, onDiskSizeWithHeader);
                HFileContextBuilder hFileContextBuilder = new HFileContextBuilder();
                hFileContextBuilder.withCompression(COMPRESSION_ALGORITHM);
                HFileContext fileContext = hFileContextBuilder.build();
                HFileBlock blk =
                        new HFileBlock(new SingleByteBuff(onDiskBlockByteBuffer), true,
                                Cacheable.MemoryType.EXCLUSIVE, offset, 0, fileContext);
                curBlock = blk;
                if(!COMPRESSION_ALGORITHM.equals(Compression.Algorithm.NONE)){
                    ByteBuff dup = blk.buf.duplicate();
                    dup.position(blk.headerSize());
                    dup = dup.slice();
                    ByteBuffInputStream byteBuffInputStream = new ByteBuffInputStream(dup);
                    InputStream dataInputStream = new DataInputStream(byteBuffInputStream);
                    Decompressor decompressor = COMPRESSION_ALGORITHM.getDecompressor();
                    CompressionCodec codec = COMPRESSION_ALGORITHM.getCodec();
                    BlockDecompressorStream blockDecompressorStream = (BlockDecompressorStream) codec.createInputStream(dataInputStream, decompressor);
                    InputStream inputStreamOfUncompress = new BufferedInputStream(blockDecompressorStream, DATA_IBUF_SIZE);
                    try {
                        int uncompressedSize = 0;
                        inputStreamOfUncompress.mark(1);
                        if(inputStreamOfUncompress.read()!=-1){
                            uncompressedSize = blockDecompressorStream.originalBlockSize;
                        }
                        inputStreamOfUncompress.reset();
                        allocateBuffer(blk,uncompressedSize);
                        IOUtils.readFully(inputStreamOfUncompress, blk.getBufferWithoutHeader().array(), blk.getBufferWithoutHeader().arrayOffset(), uncompressedSize);
                    }catch (Error error){
                        logger.debug(error);
                        continue;
                    } finally {
                        inputStreamOfUncompress.close();
                        blockDecompressorStream.close();
                        byteBuffInputStream.close();
                        dataInputStream.close();
                    }
                }else {
                    allocateBuffer(blk,onDiskSizeWithoutHeader);
                    System.arraycopy(onDiskBlockWithoutHeaderBytes,0,blk.getBufferWithoutHeader().array(),blk.getBufferWithoutHeader().arrayOffset(),onDiskSizeWithoutHeader);
                }

                blockBuffer =blk.getBufferWithoutHeader();

                logger.debug("............writing block............\n"+blk);
                do{
                    readKeyValueLen();
                    Cell cell = getCell();
                    writer.append(cell);
                    blockBuffer.skip(getCurCellSerializedSize());
                }while (blockBuffer.remaining()>0);
            }
            logger.info("***********************finish fix hfile: "+ corruptHFile.toString()+"\t--->newFile="+fixedFile.toString()+"***********");
        } finally {
            writer.close();
            fs.close();
        }
        long endTime = System.currentTimeMillis();
        logger.info("=======totalUsedTime: "+(endTime-beginTime)/1000+"s======");

    }

    /**
     * Always allocates a new buffer of the correct size. Copies header bytes
     * from the existing buffer. Does not change header fields.
     * Reserve room to keep checksum bytes too.
     */
    public void allocateBuffer(HFileBlock blk,int uncompressedSizeWithoutHeader) {
        int cksumBytes = blk.totalChecksumBytes();
        int headerSize = blk.headerSize();
        int capacityNeeded = headerSize + uncompressedSizeWithoutHeader + cksumBytes;

        // TODO we need consider allocating offheap here?
        ByteBuffer newBuf = ByteBuffer.allocate(capacityNeeded);

        // Copy header bytes into newBuf.
        // newBuf is HBB so no issue in calling array()
        blk.buf.position(0);
        blk.buf.get(newBuf.array(), newBuf.arrayOffset(), headerSize);

        blk.buf = new SingleByteBuff(newBuf);
        // set limit to exclude next block's header
        blk.buf.limit(headerSize + uncompressedSizeWithoutHeader + cksumBytes);
    }

    public void getAllBlockOffsetAndDataSize(Path file, long[] blockOffsets, int[] blockDataSizes) throws IOException {
        logger.info("-------------start compute hFile block's offset and dataSize -----------------");
        FSDataInputStream fsDataInputStream = fs.open(file);
        int count = 0;
        long offset = 0;
        long blockOffset = -1;
        byte[] oneByte = new byte[1];

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
                    //dataSize = currentBlockOffset - preBlockOffset
                    blockDataSizes[count-1] = (int)(blockOffset - blockOffsets[count-1]);
                    logger.debug("found the " + (count-1) + "th block,\toffset="+blockOffsets[count-1]+"\tdataSize="+blockDataSizes[count-1]);
                }else {
                    blockOffset = 0;
                    blockOffsets[count] = 0;
                }
                count++;
            }
        }
        int lastDataSize = BlockHeaderTools.getOnDiskSizeWithoutHeader(fsDataInputStream, blockOffset)+ HEADER_SIZE;
        blockDataSizes[count-1] = lastDataSize;
        blockDataSizes[count] = -1;
        logger.debug("found the " + (count-1) + "th block,\toffset="+blockOffsets[count-1]+"\tdataSize="+blockDataSizes[count-1]);
        fsDataInputStream.close();
        logger.info("-------------finish compute hFile block's offset and dataSize -----------------");
    }

    // Returns the #bytes in HFile for the current cell. Used to skip these many bytes in current
    // HFile block's buffer so as to position to the next cell.
    private int getCurCellSerializedSize() {
        int curCellSize =  KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen
                + currMemstoreTSLen;
        if (includesTags) {
            curCellSize += Bytes.SIZEOF_SHORT + currTagsLen;
        }
        return curCellSize;
    }

    public Cell getCell() {

        int  currTagsLen = 0;
        Cell ret;
        int cellBufSize = getKVBufSize();
        long seqId = 0L;
        if (includesMemstoreTS) {
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

    private int getKVBufSize() {
        int kvBufSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
        if (currTagsLen > 0) {
            kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
        }
        return kvBufSize;
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
        if (includesTags) {
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
        if (!includesMemstoreTS) return;
        if (!decodeMemstoreTS) {
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
                    " path=" + filePath);
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
                    + " (without header)." + ", path=" + filePath);
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
