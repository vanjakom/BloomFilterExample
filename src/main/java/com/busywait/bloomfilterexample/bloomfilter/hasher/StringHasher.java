package com.busywait.bloomfilterexample.bloomfilter.hasher;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class StringHasher implements Hasher {
    public int[] hash(long key, int numberOfHashes) {
        int[] hashes = new int[numberOfHashes];

        for (int i = 0; i < numberOfHashes; i++) {
            hashes[i] = ("" + key + i).hashCode();
        }

        return hashes;
    }
}
