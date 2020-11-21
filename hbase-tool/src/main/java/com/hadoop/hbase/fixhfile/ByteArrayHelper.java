package com.hadoop.hbase.fixhfile;


public class ByteArrayHelper {
    public static void removeFirstAndAppend(byte[] bytes,byte newV) {
        for (int i = 0; i < bytes.length-1; i++) {
            bytes[i] = bytes[i+1];
        }
        bytes[bytes.length-1] = newV;
    }

    public static void printBytes(byte[] bytes){
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<bytes.length;i++) {
            sb.append(bytes[i]).append("\t");
        }
        System.out.println(sb.toString());
    }

    /**
     * intToBytes
     * Pay attention to the high and low order
     * @param number
     * @return
     */
    public static byte[] intToBytes(int number){
        byte[] bytes = new byte[4];
        bytes[0] = (byte)((number >> 24) & 0xff);
        bytes[1] = (byte)((number >> 16) & 0xff);
        bytes[2] = (byte)((number >> 8) & 0xff);
        bytes[3] = (byte)(number & 0xff);
        return bytes;
    }

    public static byte[] longToBytes(long number) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte)((number >> 56) & 0xff);
        bytes[1] = (byte)((number >> 54) & 0xff);
        bytes[2] = (byte)((number >> 45) & 0xff);
        bytes[3] = (byte)((number >> 32) & 0xff);
        bytes[4] = (byte)((number >> 24) & 0xff);
        bytes[5] = (byte)((number >> 16) & 0xff);
        bytes[6] = (byte)((number >> 8) & 0xff);
        bytes[7] = (byte)(number & 0xff);
        return bytes;
    }
}
