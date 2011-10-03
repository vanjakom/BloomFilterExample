package com.busywait.bloomfilterexample.bloomfilter.hasher;

import com.greplin.bloomfilter.RepeatedMurmurHash;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class RepeatedMurmurHasher implements Hasher {
    protected RepeatedMurmurHash greplinHasher = null;

    public RepeatedMurmurHasher() {
        greplinHasher = new RepeatedMurmurHash();
    }

    public int[] hash(long key, int numberOfHashes) {
        return greplinHasher.hash(("" + key).getBytes(), numberOfHashes);
    }
}
