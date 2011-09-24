package com.busywait.bloomfilter.hasher;

import java.util.Random;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class RandomHasher implements Hasher {
    public int[] hash(long key, int numberOfHashes) {
        int[] hashes = new int[numberOfHashes];

        Random random = new Random(key);

        for (int i = 0; i < numberOfHashes; i++) {
            hashes[i] = random.nextInt();
        }

        return hashes;
    }
}
