package com.busywait.bloomfilterexample.bloomfilter;

import com.busywait.bloomfilterexample.bloomfilter.hasher.Hasher;
import com.busywait.bloomfilterexample.bloomfilter.hasher.RandomHasher;
import com.busywait.bloomfilterexample.bloomfilter.hasher.RepeatedMurmurHasher;
import com.busywait.bloomfilterexample.bloomfilter.hasher.StringHasher;
import com.busywait.bloomfilterexample.utils.Base64Utils;
import com.busywait.bloomfilterexample.utils.BloomUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomFilterUnitTest {
    protected static int numberOfElements = 100000;
    protected static int bitsetSize = 800000;

    @Test
    public void testWithRandomHasher() {
        testWithHasher(new RandomHasher());
    }

    @Test
    public void testWithRepeatedMurmurHasher() {
        testWithHasher(new RepeatedMurmurHasher());
    }

    @Test
    public void testWithStringHasher() {
        testWithHasher(new StringHasher());
    }

    @Test
    public void testSerialization() {
        BloomFilter filter = new BloomFilter(bitsetSize, numberOfElements, new RandomHasher());

        HashSet<Long> ids = new HashSet<Long>();

        BloomUtils.fillWithRandom(filter, ids);

        byte[] serialized = null;

        try {
             serialized = filter.getBytes();
        } catch (Exception e) {
            Assert.fail("Unable to serialize filter", e);
        }

        filter = new BloomFilter(bitsetSize, numberOfElements, new RandomHasher());

        try {
            filter.setBytes(serialized);
        } catch (Exception e) {
            Assert.fail("Unable to deserialize filter", e);
        }

        try {
            BloomUtils.checkIfExists(filter, ids);
        } catch (Exception e) {
            Assert.fail(e.getMessage(), e);
        }
    }

    @Test
    public void testStringSerialization() {
        BloomFilter filter = new BloomFilter(bitsetSize, numberOfElements, new RandomHasher());

        HashSet<Long> ids = new HashSet<Long>();

        BloomUtils.fillWithRandom(filter, ids);

        byte[] serialized = null;

        try {
             serialized = filter.getBytes();
        } catch (Exception e) {
            Assert.fail("Unable to serialize filter", e);
        }

        String serializedS = Base64Utils.fromBytes(serialized);
        byte[] deserialized = Base64Utils.fromString(serializedS);

        Assert.assertEquals(serialized, deserialized);

        filter = new BloomFilter(bitsetSize, numberOfElements, new RandomHasher());

        try {
            filter.setBytes(deserialized);
        } catch (Exception e) {
            Assert.fail("Unable to deserialize filter", e);
        }

        try {
            BloomUtils.checkIfExists(filter, ids);
        } catch (Exception e) {
            Assert.fail(e.getMessage(), e);
        }    }

    protected void testWithHasher(Hasher hasher) {
        BloomFilter filter = new BloomFilter(bitsetSize, numberOfElements, hasher);

        HashSet<Long> ids = new HashSet<Long>();

        BloomUtils.fillWithRandom(filter, ids);

        try {
            BloomUtils.checkIfExists(filter, ids);
        } catch (Exception e) {
            Assert.fail(e.getMessage(), e);
        }    }
}
