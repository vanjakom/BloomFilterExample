package com.busywait.bloomfilterexample.utils;

import com.busywait.bloomfilterexample.bloomfilter.BloomFilter;
import java.util.HashSet;
import java.util.Random;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomUtils {
    public static void fillWithRandom(BloomFilter filter, HashSet<Long> ids) {
        Random random = new Random();

        // put records inside HashSet and BloomFilter
        for (int i = 0; i < filter.getExpectedElementsNumber(); i++) {
            long key = random.nextLong();

            if (ids != null) {
                ids.add(key);
            }
            filter.add(key);
        }
    }

    public static void checkIfExists(BloomFilter filter, HashSet<Long> ids) throws Exception {
        // check if all records in HashSet are also in BloomFilter
        for (Long id: ids) {
            if (!filter.contains(id)) {
                throw new Exception("Unable to find element early added to filter");
            }
        }
    }

    public static HashSet<Long> createFalseSet(HashSet<Long> ids) {
        HashSet<Long> falseSet = new HashSet<Long>();

        Random random = new Random();
        int requiredIds = ids.size();

        while (requiredIds > 0) {
            long next = random.nextLong();

            if (!ids.contains(next)) {
                falseSet.add(next);
                requiredIds--;
            }
        }

        return falseSet;
    }
}
