package com.hadoop.hbase.fixhfile;


public class ByteArrayHelper {
    public static void removeFirstAndAppend(byte[] bytes,byte newV) {
        for (int i = 0; i < bytes.length-1; i++) {
            bytes[i] = bytes[i+1];
        }
        bytes[bytes.length-1] = newV;
    }
}
