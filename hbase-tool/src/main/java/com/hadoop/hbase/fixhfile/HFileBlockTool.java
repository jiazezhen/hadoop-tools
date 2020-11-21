package com.hadoop.hbase.fixhfile;

import com.hadoop.hbase.config.ConfigHelper;
import com.hadoop.hbase.fixmeta.ConfProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hbase.thirdparty.com.google.protobuf.UInt64Value;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

public class HFileBlockTool {
    private static final int BLOCK_SIZE = 64 * 1024;
    private static final int HEARD_SIZE = 33;
    private static final int CHECKSUM_TYPE = 2;
    public static final int BYTES_PER_CHECKSUM = 16384;

    private static FileSystem fs;
    private static Configuration conf;
    public void init(String configDir, boolean enableKrb){
        try {
            ConfProperties.setPath(configDir);
            conf = ConfigHelper.getHadoopConf(enableKrb);
            fs = ConfigHelper.getFileSystem(conf,enableKrb);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void close(){
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        HFileBlockTool hFileBlockTool = new HFileBlockTool();
        hFileBlockTool.init("/Users/zezhenjia/workSpace/gitClone/hadoop-tools/hbase-tool",true);
        Path file = new Path("/corrupHfile/corruptHfilesDemo/negativeArraySizeException/f009beb4f5e6469999affea8bf17cce8");
        hFileBlockTool.printBlockHeadersWithReverseOrder(file,-1);
        hFileBlockTool.close();
    }


    /**
     * print blockHeaders from offset with asc order
     * @param offset start offset,-1 means print from the firstDataBlockOffset
     * @param file filepath
     * @throws IOException
     */
    public void printBlockHeaders (Path file, long offset) throws IOException {
        HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true,fs.getConf());
        System.out.println("Block Headers:");

        long fileSize = fs.getFileStatus(file).getLen();
        FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, file);
        FixedFileTrailer trailer =
                FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
        System.out.println("dataIndexCount = "+trailer.getDataIndexCount());
        long max = trailer.getLastDataBlockOffset();
        if(offset == -1){
            offset = trailer.getFirstDataBlockOffset();
        }
        System.out.println("start offset="+offset);
        HFileBlock block;
        //读取所有block
        while (offset <= max) {
            block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
                    /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);
            offset += block.getOnDiskSizeWithHeader();
            System.out.println(block);
        }
        System.out.println("trailer.getLastDataBlockOffset()="+trailer.getLastDataBlockOffset());
    }

    /**
     * print blockHeaders from offset with reverse order
     *
     * @param offset start offset,-1 means print from the lastDataBlockOffset
     * @throws IOException
     */
    public void printBlockHeadersWithReverseOrder(Path file, long offset) throws IOException {
        HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, fs.getConf());

        System.out.println("Block Headers:");
        long fileSize = fs.getFileStatus(file).getLen();
        FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, file);
        FixedFileTrailer trailer =
                FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
        System.out.println("dataIndexCount = "+trailer.getDataIndexCount());
        System.out.println("entryCount = "+trailer.getEntryCount());
        if(offset == -1){
            offset = trailer.getLastDataBlockOffset();
            System.out.println("getLastDataBlockOffset=" + offset);
        }
        HFileBlock block;
        //print all blocks
        while (offset >=0) {
            block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
                    /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);
            String[] hfileItemArray = block.toString().split(",");
            String[] prevBlockOffsetKV = hfileItemArray[5].split("=");
            offset = Long.valueOf(prevBlockOffsetKV[1]);
            System.out.println(block);
            System.out.println("preoffset="+offset);

        }
        System.out.println("trailer.getFirstDataBlockOffset() ="+trailer.getFirstDataBlockOffset());


    }

    /**
     * Print specified block
     * @param file hFile path
     * @param offset dataBlock offset
     */
    public  void printPointBlock(Path file, int offset){
        try {
            HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, fs.getConf());
            HFileBlock block = reader.readBlock(offset, -1, /* cacheBlock */ false, /* pread */ false,
                    /* isCompaction */ false, /* updateCacheMetrics */ false, null, null);
            System.out.println(block);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * printBlockIndex
     * @param file
     */
    public  void printBlockIndex(Path file){
        try {
            HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, fs.getConf());
            System.out.println(reader.getDataBlockIndexReader());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public  void printTrailer(Path file) throws IOException {
        long fileSize = fs.getFileStatus(file).getLen();
        FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, file);
        FixedFileTrailer trailer =
                FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
        System.out.println(trailer);
    }

    public  void getTrailerBytes(Path file) throws IOException {
        FSDataInputStream fsd = fs.open(file);
        long fileSize = fs.getFileStatus(file).getLen();
        byte[] trailerBytes = new byte[4096];
        fsd.read(fileSize-4096,trailerBytes,0,4096);
        ByteArrayHelper.printBytes(trailerBytes);
        fsd.close();

    }

    /**
     * get onDiskSizeWithHeader from blockIndex
     * dataBlockIndexReader contains all blocks offset and dataSize(onDiskSizeWithHeader)
     * long[] blockOffsets
     * int[] blockDataSizes
     * @param file
     * @param offset
     * @return
     */
    public int getOnDiskSizeWithHeaderFromBlockIndex(Path file, long offset){
        try {
            HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, fs.getConf());
            HFileBlockIndex.BlockIndexReader dataBlockIndexReader = reader.getDataBlockIndexReader();
            long[] blockOffsets = dataBlockIndexReader.blockOffsets;
            int[] blockDataSizes = dataBlockIndexReader.blockDataSizes;

            //find the offset index
            int index = binarySearch(blockOffsets, offset);
            if(index != -1){
                System.out.println("offset="+offset+", onDiskSizeWithHeader="+blockDataSizes[index]);
                return blockDataSizes[index];
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }


    public  HFileBlockIndex.BlockIndexReader getBlockIndexReader (Path file) throws IOException {
            HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, fs.getConf());
            HFileBlockIndex.BlockIndexReader dataBlockIndexReader = reader.getDataBlockIndexReader();
            return dataBlockIndexReader;
    }

    /**
     * countUncompressedSizeWithoutHeader
     * from blockOffset+header position read onDiskSizeWithoutHeader size bytes,
     * decompress and count, get the uncompressedSizeWithoutHeader
     * @param blockOffset
     * @param onDiskSizeWithoutHeader compress size
     * @param headerSize if has checksum 33 else 24
     */
    public int countUncompressedSizeWithoutHeader(Path file, long blockOffset, int onDiskSizeWithoutHeader, int headerSize) {
        InputStream dataInputStream = null;
        InputStream isOfUncompress = null;
        int count = 0;
        System.out.println("start count uncompressedSizeWithoutHeader");
        try {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fsDataInputStream = fs.open(file);
            byte[] compressBytes = new byte[onDiskSizeWithoutHeader];
            fsDataInputStream.readFully(blockOffset + headerSize, compressBytes, 0, onDiskSizeWithoutHeader);
            ByteBuff onDiskBlock = new SingleByteBuff(ByteBuffer.wrap(compressBytes));
            final ByteBuffInputStream byteBuffInputStream = new ByteBuffInputStream(onDiskBlock);
            dataInputStream = new DataInputStream(byteBuffInputStream);
            Compression.Algorithm compressAlgo = Compression.Algorithm.SNAPPY;

            Decompressor decompressor = compressAlgo.getDecompressor();
            isOfUncompress = compressAlgo.createDecompressionStream(
                    dataInputStream, decompressor, 0);


            while (isOfUncompress.read() != -1) {
                count += 1;
            }
            System.out.println("file->"+file.toString()+"\noffset->"+blockOffset+"\nsuccess get uncompressedSizeWithoutHeader->"+count);
            return count;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dataInputStream.close();
                isOfUncompress.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return count;
        }
    }



    /**
     * correct blockType bytes
     *
     * @param blockBytes
     * @param blockTypeMagic blockType magicStr ({@link BlockType})
     * @throws IOException
     */
    public void correctBlockType (byte[] blockBytes, String blockTypeMagic) throws IOException {
        System.out.println("correct blockType="+blockTypeMagic);
        // blockType in blockHeader index 0-7, total 8 bytes
        byte[] blockTypeMagicBytes = blockTypeMagic.getBytes();
        blockBytes[0] = blockTypeMagicBytes[0];
        blockBytes[1] = blockTypeMagicBytes[1];
        blockBytes[2] = blockTypeMagicBytes[2];
        blockBytes[3] = blockTypeMagicBytes[3];
        blockBytes[4] = blockTypeMagicBytes[4];
        blockBytes[5] = blockTypeMagicBytes[5];
        blockBytes[6] = blockTypeMagicBytes[6];
        blockBytes[7] = blockTypeMagicBytes[7];
        // printBytes(blockTypeMagicBytes);
    }

    /**
     *  correct onDiskSizeWithoutHeader bytes
     *
     * @param blockBytes
     * @param onDiskSizeWithoutHeader new value
     * @throws IOException
     */
    public void correctOnDiskSizeWithoutHeader (byte[] blockBytes, int onDiskSizeWithoutHeader) throws IOException {
        System.out.println("correct onDiskSizeWithoutHeader="+onDiskSizeWithoutHeader);
        // onDiskSizeWithoutHeader in blockHeader index: 8-11
        byte[] onDiskSizeWithoutHeaderBytes = ByteArrayHelper.intToBytes(onDiskSizeWithoutHeader);
        blockBytes[8] = onDiskSizeWithoutHeaderBytes[0];
        blockBytes[9] = onDiskSizeWithoutHeaderBytes[1];
        blockBytes[10] = onDiskSizeWithoutHeaderBytes[2];
        blockBytes[11] = onDiskSizeWithoutHeaderBytes[3];
        // printBytes(onDiskSizeWithoutHeaderBytes);

    }

    /**
     * correct uncompressedSizeWithoutHeader bytes
     *
     * @param blockBytes
     * @param uncompressedSizeWithoutHeader new value
     * @throws IOException
     */
    public void correctUncompressedSizeWithoutHeader (byte[] blockBytes, int uncompressedSizeWithoutHeader) throws IOException {
        System.out.println("correct uncompressedSizeWithoutHeader="+uncompressedSizeWithoutHeader);
        // uncompressedSizeWithoutHeader in blockHeader index: 12-15
        byte[] uncompressedSizeWithoutHeaderBytes = ByteArrayHelper.intToBytes(uncompressedSizeWithoutHeader);
        blockBytes[12] = uncompressedSizeWithoutHeaderBytes[0];
        blockBytes[13] = uncompressedSizeWithoutHeaderBytes[1];
        blockBytes[14] = uncompressedSizeWithoutHeaderBytes[2];
        blockBytes[15] = uncompressedSizeWithoutHeaderBytes[3];
        // printBytes(uncompressedSizeWithoutHeaderBytes);

    }

    /**
     * correct preBlockOffset bytes
     *
     * @param blockBytes
     * @param preBlockOffset new value
     * @throws IOException
     */
    public void correctPreBlockOffset (byte[] blockBytes, long preBlockOffset) throws IOException {
        System.out.println("correct preBlockOffset="+preBlockOffset);
        // uncompressedSizeWithoutHeader in blockHeader index: 12-15
        byte[] preBlockOffsetBytes = ByteArrayHelper.longToBytes(preBlockOffset);
        blockBytes[16] = preBlockOffsetBytes[0];
        blockBytes[17] = preBlockOffsetBytes[1];
        blockBytes[18] = preBlockOffsetBytes[2];
        blockBytes[19] = preBlockOffsetBytes[3];
        blockBytes[20] = preBlockOffsetBytes[4];
        blockBytes[21] = preBlockOffsetBytes[5];
        blockBytes[22] = preBlockOffsetBytes[6];
        blockBytes[23] = preBlockOffsetBytes[7];
        // printBytes(preBlockOffsetBytes);

    }


    /**
     * correct checksumType bytes
     *
     * @param blockBytes
     * @param checksumType （{@link ChecksumType}）
     * @throws IOException
     */
    public void correctChecksumType (byte[] blockBytes, int checksumType) throws IOException {
        System.out.println("correct checksumType="+checksumType);
        //checksumType in blockHeader index 24
        blockBytes[24]=(byte) checksumType;
    }

    /**
     * correct bytesPerChecksum bytes
     *
     * @param blockBytes
     * @param bytesPerChecksum new value
     * @throws IOException
     */
    public void correctBytesPerChecksum (byte[] blockBytes, int bytesPerChecksum) throws IOException {
        System.out.println("correct bytesPerChecksum=" + bytesPerChecksum);
        // onDiskDataSizeWithHeader in blockHeader index: 25-28
        byte[] bytesPerChecksumBytes = ByteArrayHelper.intToBytes(bytesPerChecksum);
        blockBytes[25] = bytesPerChecksumBytes[0];
        blockBytes[26] = bytesPerChecksumBytes[1];
        blockBytes[27] = bytesPerChecksumBytes[2];
        blockBytes[28] = bytesPerChecksumBytes[3];
        // printBytes(bytesPerChecksumBytes);
    }

    /**
     * correct  onDiskDataSizeWithHeader bytes
     *
     * @param blockBytes
     * @param onDiskDataSizeWithHeader new value
     * @throws IOException
     */
    public void correctOnDiskDataSizeWithHeader (byte[] blockBytes, int onDiskDataSizeWithHeader) throws IOException {
        System.out.println("correct onDiskDataSizeWithHeader=" + onDiskDataSizeWithHeader);
        // onDiskDataSizeWithHeader in blockHeader index: 29-32
        byte[] onDiskDataSizeWithHeaderBytes = ByteArrayHelper.intToBytes(onDiskDataSizeWithHeader);
        blockBytes[29] = onDiskDataSizeWithHeaderBytes[0];
        blockBytes[30] = onDiskDataSizeWithHeaderBytes[1];
        blockBytes[31] = onDiskDataSizeWithHeaderBytes[2];
        blockBytes[32] = onDiskDataSizeWithHeaderBytes[3];
        // printBytes(onDiskDataSizeWithHeaderBytes);
    }

    /**
     * blockIndex contains all blockOffsets and blockDataSizes
     * The premise is that blockIndex has no missing parts and the trailer is ok
     * @param corruptFile
     * @param fixedFile
     * @throws IOException
     */
    public void fixAllBlockHeaders(Path corruptFile, Path fixedFile) throws IOException {
        long beginTime = System.currentTimeMillis();
        HFile.Reader reader = HFile.createReader(fs, corruptFile, CacheConfig.DISABLED, true, fs.getConf());
        HFileBlockIndex.BlockIndexReader blockIndexReader = reader.getDataBlockIndexReader();
        long[] blockOffsets = blockIndexReader.blockOffsets;
        int[] blockDataSizes = blockIndexReader.blockDataSizes;
        if(blockOffsets.length<0 || blockDataSizes.length<0 || blockDataSizes.length!=blockDataSizes.length){
            System.out.println("Error: blockOffsets or blockDataSizes has corrupt");
            return;
        }
        int blockLength = blockOffsets.length;

        //verify if blockIndex offset equal Trailer dataBlockOffset
        long fileSize = fs.getFileStatus(corruptFile).getLen();
        FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, corruptFile);
        FixedFileTrailer trailer =
                FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
        long firstDataBlockOffset = trailer.getFirstDataBlockOffset();
        long lastDataBlockOffset = trailer.getLastDataBlockOffset();
        if(blockOffsets[0] != firstDataBlockOffset || blockOffsets[blockLength-1] != lastDataBlockOffset){
            System.out.println("Error: Trailer or BlockIndex has corrupt");
            return;
        }

        System.out.println("start fix offset and onDiskSizeWithoutHeader......");

        FSDataInputStream dataInputStream = fs.open(corruptFile);
        FSDataOutputStream dataOutputStream = fs.create(fixedFile);


        for (int i = 0; i < blockLength ; i++) {
            byte[] blockBytes = new byte[blockDataSizes[i]];
            dataInputStream.read(blockOffsets[i],blockBytes,0,blockDataSizes[i]);

            int onDiskSizeWithoutHeader = blockDataSizes[i] - HEARD_SIZE;
            correctOnDiskSizeWithoutHeader(blockBytes,onDiskSizeWithoutHeader);
            correctBlockType(blockBytes,"DATABLK*");
            if(i>0){
                correctPreBlockOffset(blockBytes,blockOffsets[i-1]);
            }else {
                correctPreBlockOffset(blockBytes,-1);
            }
            int onDiskDataSizeWithHeader = countOnDiskDataSizeWithHeader(onDiskSizeWithoutHeader);
            correctOnDiskDataSizeWithHeader(blockBytes,onDiskDataSizeWithHeader);
            int uncompressedSizeWithoutHeader =
                    countUncompressedSizeWithoutHeader(corruptFile, blockOffsets[i], onDiskSizeWithoutHeader, HEARD_SIZE);
            correctUncompressedSizeWithoutHeader(blockBytes, uncompressedSizeWithoutHeader);
            correctChecksumType(blockBytes,CHECKSUM_TYPE);
            correctBytesPerChecksum(blockBytes,BYTES_PER_CHECKSUM);
            dataOutputStream.write(blockBytes);

        }

        long limitOffset = lastDataBlockOffset+blockDataSizes[blockLength-1];
        dataInputStream.seek(limitOffset);
        IOUtils.copyBytes(dataInputStream,dataOutputStream,fileSize-limitOffset,false);

        System.out.println("finish fix offset and onDiskSizeWithoutHeader!!!!");
        long endTime = System.currentTimeMillis();
        System.out.println("TotalTime: "+(endTime-beginTime)/1000+"s");
        dataInputStream.close();
        dataOutputStream.close();

    }

    /**
     * fix blockHeader
     * @param corruptFile
     * @param fixedFile
     * @param curBlockOffsetL broken blockHeader's next block
     * @param preBlockOffsetL this block's blockHeader was broken
     * @param hdrSize
     * @throws IOException
     */
    public void fixBlockHeaderWithReverseOrder(Path corruptFile, Path fixedFile, long curBlockOffsetL,
                                                            long preBlockOffsetL, int hdrSize) throws IOException {
        System.out.println("fix blockHeader  begin...");
        long beginTime = System.currentTimeMillis();
        FSDataOutputStream fsDataOutputStream = null;
        FSDataInputStream fsDataInputStream = null;

        /**
         * [blockType=DATA, fileOffset=263193020, headerSize=33, onDiskSizeWithoutHeader=15382,
         * uncompressedSizeWithoutHeader=65604, prevBlockOffset=263177650, isUseHBaseChecksum=true, checksumType=CRC32C,
         * bytesPerChecksum=16384, onDiskDataSizeWithHeader=15411, getOnDiskSizeWithHeader=15415, totalChecksumBytes=4,
         * isUnpacked=true, buf=[SingleByteBuff[pos=0, lim=65641, cap= 65641]],
         * dataBeginsWith=\x00\x00\x00.\x00\x00\x00"\x00\x1F933067612_133543_17232,
         * fileContext=[usesHBaseChecksum=true, checksumType=CRC32C, bytesPerChecksum=16384, blocksize=65536,
         * encoding=NONE, includesMvcc=true, includesTags=false, compressAlgo=SNAPPY, compressTags=false,
         * cryptoContext=[cipher=NONE keyHash=NONE], name=f009beb4f5e6469999affea8bf17cce8], nextBlockOnDiskSize=15385]
         */
        int curBlockOffset = (int)curBlockOffsetL; // larger
        int preBlockOffset = (int) preBlockOffsetL; // smaller

        int onDiskSizeWithHeader = curBlockOffset - preBlockOffset;
        int onDiskSizeWithoutHeader = onDiskSizeWithHeader - hdrSize;

        try {
            fsDataOutputStream = fs.create(fixedFile);
            long fileSize = fs.getFileStatus(corruptFile).getLen();
            fsDataInputStream = fs.open(corruptFile);
            System.out.println("fileSize="+fileSize);

            //[0,preBlockOffset) write
            byte[] firstBytes = new byte[preBlockOffset];
            fsDataInputStream.read(0,firstBytes,0,preBlockOffset);
            fsDataOutputStream.write(firstBytes);
            System.out.println("[0, "+preBlockOffset+"), write finished");

            //[preBlockOffset,preBlockOffset+onDiskSizeWithHeader) write
            byte[] blockBytes = new byte[onDiskSizeWithHeader];
            fsDataInputStream.read(preBlockOffsetL,blockBytes,0,onDiskSizeWithHeader);

            correctBlockType(blockBytes, "DATABLK*");
            correctOnDiskSizeWithoutHeader(blockBytes, onDiskSizeWithoutHeader);
            int uncompressedSizeWithoutHeader =
                    countUncompressedSizeWithoutHeader(corruptFile, preBlockOffset, onDiskSizeWithoutHeader, hdrSize);
            correctUncompressedSizeWithoutHeader(blockBytes, uncompressedSizeWithoutHeader);
            correctChecksumType(blockBytes,2);
            correctBytesPerChecksum(blockBytes,BYTES_PER_CHECKSUM);
            int onDiskDataSizeWithHeader = countOnDiskDataSizeWithHeader(onDiskSizeWithoutHeader);
            correctOnDiskDataSizeWithHeader(blockBytes,onDiskDataSizeWithHeader);
            fsDataOutputStream.write(blockBytes);
            System.out.println("["+preBlockOffset+", "+(preBlockOffset+onDiskSizeWithHeader)+"), write finished");

            //[preBlockOffset+onDiskSizeWithHeader,filesize) write
            int last = new Long(fileSize - curBlockOffset).intValue();
            byte[] lastBytes = new byte[last];
            fsDataInputStream.read(curBlockOffset,lastBytes,0,last);
            fsDataOutputStream.write(lastBytes);
            System.out.println("["+curBlockOffset+", "+fileSize+"), write finished");

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fsDataInputStream!=null && fsDataOutputStream!=null){
                fsDataInputStream.close();
                fsDataOutputStream.close();
            }
        }
        System.out.println("fix blockheader success");
        long endTime = System.currentTimeMillis();
        System.out.println("TotalTime="+(endTime-beginTime)/1000+"s");
    }

    public  void fixTrailer(Path corruptFile, Path fixFile) throws IOException {
        System.out.println("fix trailer block begin...");
        long beginTime = System.currentTimeMillis();
        FileSystem fs;
        FSDataOutputStream fsDataOutputStream = null;
        FSDataInputStream fsDataInputStream = null;
        //trailer
        try {
            fs = FileSystem.get(conf);
            fsDataOutputStream = fs.create(fixFile);

            long fileSize = fs.getFileStatus(corruptFile).getLen();
            fsDataInputStream = fs.open(corruptFile);
            System.out.println("filesize="+fileSize);
            long limit = fileSize - 4096;

            IOUtils.copyBytes(fsDataInputStream,fsDataOutputStream,limit,false);
            System.out.println("copyBytes end");

            byte[] trailerbytes = new byte[4096];
            fsDataInputStream.read(limit,trailerbytes,0,4096);

            /**
             * now can only fix trailer blockType fileInfoOffset loadOnOpenDataOffset and version
             * trailerbytes[8] = sizeof(fileInfoOffset + loadOnOpenDataOffset + uncompressedDataIndexSize + ....encryptionKey)
             */
            {
                // trailer blockType is TRABLK\"$
                trailerbytes[0] = 84;
                trailerbytes[1] = 82;
                trailerbytes[2] = 65;
                trailerbytes[3] = 66;
                trailerbytes[4] = 76;
                trailerbytes[5] = 75;
                trailerbytes[6] = 34;
                trailerbytes[7] = 36;
                System.out.println("correct blocktype end");

                // size
                //trailerbytes[8] = 84;
                int nextIndex = 9;
                // fileinfoOffset
                trailerbytes[nextIndex] = 8;
                long fileinfoOffset = countBlockOffset(corruptFile, "FILEINF2", fileSize / 2);
                byte[] fileinfo_bytes = UInt64Value.of(fileinfoOffset).toByteArray();
                changeTrailerBytes(trailerbytes,fileinfo_bytes,nextIndex);
                nextIndex += fileinfo_bytes.length;
                System.out.println("correct fileinfoOffset end");


                // loadOnOpenDataOffset
                trailerbytes[nextIndex] = 16;
                long loadOnOpenDataOffset = countBlockOffset(corruptFile, "IDXROOT2", fileSize / 2);
                byte[] loadOnOpenDataOffset_bytes = UInt64Value.of(loadOnOpenDataOffset).toByteArray();
                changeTrailerBytes(trailerbytes,loadOnOpenDataOffset_bytes,nextIndex);
                nextIndex += loadOnOpenDataOffset_bytes.length;
                System.out.println("correct loadOnOpenDataOffset end");

                // uncompressedDataIndexSize
                trailerbytes[nextIndex] = 24;
                // totalUncomressedBytes
                // dataIndexCount
                // metaIndexCount
                // entryCount
                // numDataIndexLevels
                // firstDataBlockOffset
                // lastDataBlockOffset
                // comparatorClassName
                // compressionCodec
                // encryptionKey

                //version
                trailerbytes[4092] = 3;
                trailerbytes[4093] = 0;
                trailerbytes[4094] = 0;
                trailerbytes[4095] = 3;
                System.out.println("correct version end");



            }
            fsDataOutputStream.write(trailerbytes);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fsDataInputStream!=null && fsDataOutputStream!=null){
                fsDataInputStream.close();
                fsDataOutputStream.close();
            }
        }
        System.out.println("fix trailer block success");
        long endTime = System.currentTimeMillis();
        System.out.println("totalTime: "+(endTime-beginTime)/1000+"s");

    }

    private void changeTrailerBytes(byte[] trailer_bytes, byte[] value_bytes, int index) {
        //value_bytes第一位是8，不要
        for (int j = 1; j < value_bytes.length ; j++) {
            trailer_bytes[index+j] = value_bytes[j];
        }
    }

    /**
     * onDiskDataSizeWithHeader=onDiskSizeWithoutHeader + headerSize - totalChecksumBytes
     * but the totalChecksumBytes is not sure，4 or 8
     * @param onDiskSizeWithoutHeader
     * @return
     */
    public  int countOnDiskDataSizeWithHeader(int onDiskSizeWithoutHeader){
        int totalChecksumBytes;
        int numChunks = 0;
        //assume 4
        int supperTotalChecksumBytes = 4;
        int onDiskDataSizeWithHeader=onDiskSizeWithoutHeader + HEARD_SIZE - supperTotalChecksumBytes;

        numChunks = onDiskDataSizeWithHeader/BYTES_PER_CHECKSUM;

        if(onDiskDataSizeWithHeader%BYTES_PER_CHECKSUM!=0){
            numChunks = numChunks + 1;
        }
        totalChecksumBytes = numChunks * 4;
        // totalChecksumBytes = 4 is right
        if(totalChecksumBytes == supperTotalChecksumBytes){
            return onDiskDataSizeWithHeader;
        }else {
            // totalChecksumBytes = 8
            return onDiskSizeWithoutHeader + HEARD_SIZE - 8;
        }
    }

    /**
     * 知道当前block的offset，但不知道当前block的大小，所以需要自己去根据blcok的起始标记试着去找下一个block的offset
     * 仅当假设下一个block的blocktype没被损坏时
     * @param offset 起始block offset，当找最后一个blockoffset时，给-1程序会选3/4位置开始查找以优化查询速度
     * @param file
     * @param index 从offset开始查，找到第几个，1表示第一个，-1直到最后一个
     * @throws IOException
     */
    public long countNextDataBlockOffset(Path file, long offset, int index) throws IOException{
        FSDataInputStream fsDataInputStream = fs.open(file);
        long fileSize = fs.getFileStatus(file).getLen();
        System.out.println("fileSize = "+ fileSize);
        fsDataInputStream.seek(offset);
        int count = 0;
        long blockOffset = -1;
        byte[] oneByte = new byte[1];
        byte[] windowBytes = new byte[8];
        while (fsDataInputStream.read(oneByte)!=-1 ){
            if(index==1 ||(index!=-1 && count >=index)){
                break;
            }
            offset +=1;
            ByteArrayHelper.removeFirstAndAppend(windowBytes,oneByte[0]);
            //DATABLK* 的bytes为{68，65，84，65，66，76，75，42}
            boolean isDataBlock = Arrays.equals(windowBytes, "DATABLK*".getBytes());
            if(isDataBlock){
                count++;
                blockOffset = offset - 8;
                System.out.println("from "+ offset + " start, found the " + count + "th blockoffset="+blockOffset);

            }
        }

        fsDataInputStream.close();
        System.out.println("the final blockOffset=" + blockOffset);
        return blockOffset;

    }

    /**
     * this func help you find block offset
     * if the blockType has broken, will not found or not the first one
     * @param file
     * @param blockTypeMagic
     * @param offset search start from offset
     * @throws IOException
     */
    public long countBlockOffset(Path file, String blockTypeMagic, long offset) throws IOException {
        long filesize = fs.getFileStatus(file).getLen();
        System.out.println("filesize = "+filesize);
        FSDataInputStream fsDataInputStream = fs.open(file);
        //search start offset
        fsDataInputStream.seek(offset);
        System.out.println("seek position ： "+ fsDataInputStream.getPos());
        long blockOffset = 0;

        byte[] blockTypeBytes = blockTypeMagic.getBytes();
        byte[] oneByte = new byte[1];
        boolean flag = false;
        byte[] windowBytes = new byte[8];
        while (fsDataInputStream.read(oneByte)!=-1 ){
            offset +=1;
            ByteArrayHelper.removeFirstAndAppend(windowBytes,oneByte[0]);
            boolean isBlockType = Arrays.equals(windowBytes, blockTypeBytes);
            if(isBlockType){
                flag = true;
                blockOffset = offset - 8;
                break;
            }
        }

        if(flag){
            System.out.println("blockOffset=" + blockOffset);
        }else {
            System.out.println(blockTypeMagic+" not found");
        }
        fsDataInputStream.close();
        return blockOffset;

    }


    private int binarySearch(long[] blockOffsets, long value){
        int low,high,mid;
        low = 0;
        high = blockOffsets.length-1;
        while (low<high){
            mid = (low + high)/2;
            if(blockOffsets[mid] == value){
                return mid;
            }
            if(blockOffsets[mid] > value){
                high = mid - 1;
            }
            if(blockOffsets[mid] < value){
                low = mid + 1;
            }
        }
        return -1;
    }

}
