package com.gimral.transforms.connect.util;

import org.apache.kafka.connect.errors.DataException;

public class Requirements {
    public static byte[] requireJson(Object value, String purpose) {
        if(value instanceof String){
            return ((String) value).getBytes();
        }
        if (value instanceof byte[]){
            return (byte[]) value;
        }
        throw new DataException("Only Json object representations of byte array and String are supported for [" + purpose + "]" + nullSafeClassName(value));
    }

    public static byte[] requireByteArray(Object value, String purpose) {
        if (!(value instanceof byte[])) {
            throw new DataException("Only byte array objects supported for [" + purpose + "], found: " + nullSafeClassName(value));
        }
        return (byte[]) value;
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }
}
