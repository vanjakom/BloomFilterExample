package com.busywait.bloomfilterexample.bloomfilter;

import com.busywait.bloomfilterexample.bloomfilter.hasher.Hasher;
import com.busywait.bloomfilterexample.bloomfilter.hasher.RandomHasher;
import com.busywait.bloomfilterexample.bloomfilter.hasher.RepeatedMurmurHasher;
import com.busywait.bloomfilterexample.bloomfilter.hasher.StringHasher;
import com.busywait.bloomfilterexample.utils.BloomUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomFilterPerformanceTest {
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

        try {
            BloomUtils.checkIfExists(filter, ids);
        } catch (Exception e) {
            Assert.fail(e.getMessage(), e);
        }

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
