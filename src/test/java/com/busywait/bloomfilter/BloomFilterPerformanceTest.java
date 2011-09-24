package com.busywait.bloomfilter;

import com.busywait.bloomfilter.hasher.Hasher;
import com.busywait.bloomfilter.hasher.RandomHasher;
import com.busywait.bloomfilter.hasher.RepeatedMurmurHasher;
import com.busywait.bloomfilter.hasher.StringHasher;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomFilterPerformanceTest {
    //public static int[] bitSetSizes = {200000, 400000, 800000};
    //public static int[] numberOfElements = {100000, 100000, 100000};
    public static int[] bitSetSizes = {2000000, 4000000, 8000000};
    public static int[] numberOfElements = {1000000, 1000000, 1000000};

    @Test
    public void testFalsePossitiveHashers() {
        for (int i = 0; i < bitSetSizes.length; i++) {
            testFalsePossitive(bitSetSizes[i], numberOfElements[i], new RandomHasher());
        }

        for (int i = 0; i < bitSetSizes.length; i++) {
            testFalsePossitive(bitSetSizes[i], numberOfElements[i], new RepeatedMurmurHasher());
        }

        for (int i = 0; i < bitSetSizes.length; i++) {
            testFalsePossitive(bitSetSizes[i], numberOfElements[i], new StringHasher());
        }
    }

    protected void testFalsePossitive(int bitsetSize, int numberOfElements, Hasher hasher) {
        BloomFilter filter = new BloomFilter(bitsetSize, numberOfElements, hasher);

        HashSet<Long> ids = new HashSet<Long>();

        BloomUtils.fillWithRandom(filter, ids);

        BloomUtils.checkIfExists(filter, ids);

        // test false possitive
        int falsePossitiveCount = 0;
        HashSet<Long> falseSet = BloomUtils.createFalseSet(ids);

        for (Long falseId: falseSet) {
            if (filter.contains(falseId)) {
                falsePossitiveCount++;
            }
        }

        StringBuilder sb = new StringBuilder();

        System.out.println("*** Hasher used: " + hasher.getClass() + " ***");
        System.out.println("BitSet size: " + bitsetSize + "(" + (bitsetSize/8/1024) + " kb)");
        System.out.println("False positive probability: " + filter.falsePositiveProbability());
        System.out.println("Number of elements: " + numberOfElements);
        System.out.println("False positive count: " + falsePossitiveCount + " from: " + numberOfElements +
            " percentage: " + (int)((float)falsePossitiveCount / numberOfElements * 100) + "%");
    }
}
