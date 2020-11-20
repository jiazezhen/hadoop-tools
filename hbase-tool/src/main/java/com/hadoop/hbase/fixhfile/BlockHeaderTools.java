package com.hadoop.hbase.fixhfile;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockHeaderTools {

    public static String getBlockType(FSDataInputStream inputStream, long blockOffset) throws IOException {
        //blockType 保存在Header信息的index为0，占8bytes
        byte[] blockType = new byte[8];
        inputStream.read(blockOffset,blockType,0,8);
        String result = new String(blockType);
        System.out.println(" blockType = " + result);
        return result;

    }

    public static int getOnDiskSizeWithoutHeader(FSDataInputStream inputStream, long blockOffset) throws IOException {
        //onDiskDataSizeWithHeader 保存在Header信息的index为29，占4bytes
        byte[] onDiskSizeWithoutHeader = new byte[4];
        inputStream.read(blockOffset + 8,onDiskSizeWithoutHeader,0,4);
        int result = ByteBuffer.wrap(onDiskSizeWithoutHeader).getInt();
        System.out.println(" onDiskSizeWithoutHeader = " + result);
        return result;

    }

    public static long getUncompressedSizeWithoutHeader(FSDataInputStream inputStream, long blockOffset) throws IOException {
        // uncompressedSizeWithoutHeader在blockheader中index为12-15
        byte[] uncompressedSizeWithoutHeader = new byte[4];
        inputStream.read(blockOffset + 12,uncompressedSizeWithoutHeader,0,4);
        int result = ByteBuffer.wrap(uncompressedSizeWithoutHeader).getInt();
        System.out.println(" uncompressedSizeWithoutHeader = " + result);
        return result;

    }

    /**
     * 读取当前block的header信息，获取preBlockOffset
     * @param inputStream
     * @param blockOffset
     * @return
     */
    public static long getPreBlockOffset(FSDataInputStream inputStream, long blockOffset) throws IOException {
        //preBlockOffset 保存在Header信息的index为16，占8bytes
        byte[] preBlockOffsetBytes = new byte[8];
        inputStream.read(blockOffset+16,preBlockOffsetBytes,0,8);
        long result = ByteBuffer.wrap(preBlockOffsetBytes).getLong();
        System.out.println(" preBlockOffset = " + result);
        return result;

    }

    public static byte getChecksumType(FSDataInputStream inputStream, long blockOffset) throws IOException {
        //ChecksumType 保存在Header信息的index为24，占1 bytes
        byte[] checksumType = new byte[1];
        inputStream.read(blockOffset + 24,checksumType,0,1);
        System.out.println(" checksumType = " + checksumType[0]);
        return checksumType[0];

    }

    public static long getBytesPerChecksum(FSDataInputStream inputStream, long blockOffset) throws IOException {
        //bytesPerChecksum 保存在Header信息的index为25，占4bytes
        byte[] bytesPerChecksum = new byte[4];
        inputStream.read(blockOffset + 25,bytesPerChecksum,0,4);
        int result =ByteBuffer.wrap(bytesPerChecksum).getInt();
        System.out.println(" bytesPerChecksum = " + result);
        return result;

    }

    public static long getOnDiskDataSizeWithHeader(FSDataInputStream inputStream, long blockOffset) throws IOException {
        //onDiskDataSizeWithHeader 保存在Header信息的index为29，占4bytes
        byte[] onDiskDataSizeWithHeader = new byte[4];
        inputStream.read(blockOffset + 29,onDiskDataSizeWithHeader,0,4);
        int result =ByteBuffer.wrap(onDiskDataSizeWithHeader).getInt();
        System.out.println(" onDiskDataSizeWithHeader = " + result);
        return result;

    }




}
