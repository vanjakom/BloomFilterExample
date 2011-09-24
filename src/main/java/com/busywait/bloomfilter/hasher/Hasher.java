package com.busywait.bloomfilter.hasher;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public interface Hasher {
    public int[] hash(long key, int numberOfHashes);
}
