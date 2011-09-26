package com.busywait.utils;

import org.apache.commons.codec.binary.Base64;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class Base64Utils {
    public static String fromBytes(byte[] bytes) {
        return new String(Base64.encodeBase64(bytes));
    }

    public static byte[] fromString(String string) {
        return Base64.decodeBase64(string);
    }

    public static byte[] fromBase64(byte[] base64bytes) {
        return Base64.decodeBase64(base64bytes);
    }

    public static byte[] toBase64(byte[] bytes) {
        return Base64.encodeBase64(bytes);
    }
}
