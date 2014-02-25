package com.lidl.hadoop;

/**
 * Created by worker on 2/24/14.
 */
public class DBHandler {
    private static final String connectionStr = "mongodb://zonlolo:zonlolo_P2ssW0rd@192.168.1.100/zonlolo";
    public static String getCollection(String collectionName) {
        return connectionStr.concat(".").concat(collectionName);
    }
}
