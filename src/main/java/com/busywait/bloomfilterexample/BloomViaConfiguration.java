package com.busywait.bloomfilterexample;

import com.busywait.bloomfilterexample.bloomfilter.BloomFilter;
import com.busywait.bloomfilterexample.bloomfilter.hasher.RandomHasher;
import com.busywait.bloomfilterexample.mapreduce.BloomViaConfigurationTask;
import com.busywait.bloomfilterexample.utils.BloomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomViaConfiguration {
    protected static int numberOfElements = 10000;
    protected static int bitsetSize = 80000;

    public static String input_path_hdfs = "hdfs://localhost/temp/input_records";
    public static String input_path_ids_hdfs = "hdfs://localhost/temp/input_ids";
    public static String output_path_hdfs = "hdfs://localhost/temp/output_configuration";

    public static void main(String[] args) {
        // write sample data to HDFS, create BloomFilter and ids hashset
        BloomFilter filter = new BloomFilter(bitsetSize, numberOfElements, new RandomHasher());
        HashSet<Long> ids = new HashSet<Long>();

        BloomUtils.fillWithRandom(filter, ids);
        HashSet<Long> falseIds = BloomUtils.createFalseSet(ids);

        // write input data
        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost/"), new Configuration());

            // write input data
            FSDataOutputStream stream = fs.create(new Path(input_path_hdfs));
            for (Long id: ids) {
                stream.write((id + "\tdata\toriginal data\n").getBytes());
            }
            for (Long id: falseIds) {
                stream.write((id + "\tdata\toriginal data\n").getBytes());
            }
            stream.close();

            // write ids
             // write input data
            stream = fs.create(new Path(input_path_ids_hdfs));
            for (Long id: ids) {
                stream.write((id + "\tid\n").getBytes());
            }
            stream.close();
        } catch (Exception e) {
            System.err.println("Unable to upload input file to hdfs");
            e.printStackTrace();
            System.exit(1);
        }

        HashMap<String, String> additionalConf = new HashMap<String, String>();
        additionalConf.put("mapred.reduce.tasks", "1");

        // run map reduce
        BloomViaConfigurationTask task = new BloomViaConfigurationTask();
        try {
            task.execute(filter, new String[] {input_path_hdfs, input_path_ids_hdfs}, output_path_hdfs, additionalConf);
        } catch (Exception e) {
            System.err.println("Unable to execute map reduce task");
            e.printStackTrace();
            System.exit(1);
        }

        // test if output contains all ids, calculate false positive count
        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost/"), new Configuration());

            FSDataInputStream stream = fs.open(new Path(output_path_hdfs + "/part-r-00000"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split("\t");
                Long key = Long.parseLong(elements[0]);

                ids.remove(key);
            }
            stream.close();
        } catch (Exception e) {
            System.err.println("Unable to test output");
            e.printStackTrace();
            e.printStackTrace();
            System.exit(1);
        }

        if (ids.size() > 0) {
            System.err.println("Not all ids found in output, number of ids: " + ids.size());
            System.exit(1);
        }
    }
}
