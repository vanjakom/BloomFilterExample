package com.busywait.bloomfilter;

import com.busywait.bloomfilter.hasher.Hasher;

import java.io.IOException;
import java.util.BitSet;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomFilter {
    protected int bitSetSize = 0;
    protected int elementsNumber = 0;
    protected int numberOfHashes = 0;

    protected BitSet bitSet = null;
    protected Hasher hasher = null;

    public BloomFilter(int bitSetSize, int elementsNumber, Hasher hasher) {
        this.bitSetSize = bitSetSize;
        this.elementsNumber = elementsNumber;

        this.bitSet = new BitSet(bitSetSize);
        this.hasher = hasher;

        //calculate number of hash function required to store element
        numberOfHashes = (int)Math.round(Math.log(2.0) * bitSetSize / elementsNumber);
    }

    public void add(long key) {
        int[] hashes = hasher.hash(key, numberOfHashes);

        for (int i = 0; i < numberOfHashes; i++) {
            int position = convertHashToPosition(hashes[i]);

            bitSet.set(position, true);
        }
    }

    public boolean contains(long key) {
        int[] hashes = hasher.hash(key, numberOfHashes);

        for (int i = 0; i < numberOfHashes; i++) {
            int position = convertHashToPosition(hashes[i]);

            if (!bitSet.get(position)) {
                return false;
            }
        }

        return true;
    }

    public byte[] getBytes() throws IOException {
        byte[] bytes = new byte[bitSetSize / 8];

        for ( int i = 0 ; i < bitSetSize / 8; i++) {
            byte nextElement = 0;
            for (int j = 0; j < 8; j++) {
                if (bitSet.get(8 * i + j)) {
                    nextElement |= 1<<j;
                }
            }

            bytes[i] = nextElement;
        }

        return bytes;
    }

    public void setBytes(byte[] bytes) throws IOException {
        bitSet.clear();

        for (int i = 0; i < bitSetSize / 8; i++) {
            byte nextByte = bytes[i];

            for (int j = 0; j < 8; j++) {
                if (((int)nextByte & (1 <<j)) != 0) {
                    bitSet.set(8 * i + j);
                }
            }
        }
    }

    public double falsePositiveProbability() {
        return Math.pow((1 - Math.exp(-numberOfHashes * (double) elementsNumber / (double) bitSetSize)), numberOfHashes);
    }

    public int getBitSetSize() {
        return bitSetSize;
    }

    public int getExpectedElementsNumber() {
        return elementsNumber;
    }

    protected int convertHashToPosition(int hash) {
        return Math.abs(hash) % bitSetSize;
    }
}
