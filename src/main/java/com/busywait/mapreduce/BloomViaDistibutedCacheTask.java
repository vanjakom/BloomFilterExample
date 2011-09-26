package com.busywait.mapreduce;

import com.busywait.bloomfilter.BloomFilter;
import com.busywait.bloomfilter.hasher.Hasher;
import com.busywait.utils.Base64Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class BloomViaDistibutedCacheTask {
    public static final String COUNTER_DICARDED_BY_BLOOM = "discarded_by_bloom";

    public static final String CONF_BLOOM_EXPECTED_ELEMENTS = "bloom_bitset_expected_elements";
    public static final String CONF_BLOOM_BITSET_SIZE = "bloom_bitset_size";
    public static final String CONF_HASHER_CLASS = "hasher_class";

    public static class BloomFilteringMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected BloomFilter filter = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            int expectedElements = Integer.parseInt(context.getConfiguration().get(CONF_BLOOM_EXPECTED_ELEMENTS));
            int bitSetSize = Integer.parseInt(context.getConfiguration().get(CONF_BLOOM_BITSET_SIZE));

            BufferedReader reader = new BufferedReader(new FileReader("bloom_filter"));
            String bloomSerialized = reader.readLine();
            reader.close();

            byte[] bitsetBytes = bloomSerialized.getBytes(Charset.forName("UTF-8"));

            Hasher hasher = null;

            try {
                hasher = (Hasher)Class.forName(context.getConfiguration().get(CONF_HASHER_CLASS)).newInstance();
            } catch (Exception e) {
                throw new IOException("Unable to setup hasher for class: " + context.getConfiguration().get(CONF_HASHER_CLASS));
            }

            filter = new BloomFilter(bitSetSize, expectedElements, hasher);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // entry should be in following format: id \t type \t data
            // type should be data for data records
            // type should be id for id records
            String[] entries = value.toString().split("\t");

            if (filter.contains(Long.parseLong(entries[0]))) {
                context.write(new Text(entries[0]), value);
            } else {
                context.getCounter(BloomViaConfigurationTask.class.getName(), COUNTER_DICARDED_BY_BLOOM).increment(1);
            }
        }
    }

    public static class BloomFilteringReduce  extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();

            boolean hasId = false;
            String record = null;

            while (iterator.hasNext()) {
                String current = iterator.next().toString();
                String[] entries = current.split("\t");

                if (entries[1].equals("id")) {
                    hasId = true;
                } else {
                    record = current;
                }
            }

            context.write(null, new Text(record));
        }
    }

    public void execute(BloomFilter filter, String[] inputPaths, String outputPath,
                        HashMap<String, String> additionalConf) throws Exception {
        Configuration configuration = new Configuration();

        Job job = new Job(configuration);
        job.setJarByClass(BloomViaConfigurationTask.class);
        for (String inputPath: inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(BloomFilteringMapper.class);
        job.setReducerClass(BloomFilteringReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost/"), new Configuration());
        FSDataOutputStream outputStream = fs.create(new Path("hdfs://localhost/temp/bloom_filter"));
        outputStream.write(Base64Utils.fromBytes(filter.getBytes()).getBytes());
        outputStream.close();

        DistributedCache.addFileToClassPath(new Path("/temp/bloom_filter"), new Configuration());

        job.getConfiguration().set(CONF_BLOOM_BITSET_SIZE, "" + filter.getBitSetSize());
        job.getConfiguration().set(CONF_BLOOM_EXPECTED_ELEMENTS, "" + filter.getExpectedElementsNumber());
        job.getConfiguration().set(CONF_HASHER_CLASS, "" + filter.getHasher().getClass().getName());

        for (String key: additionalConf.keySet()) {
            job.getConfiguration().set(key, additionalConf.get(key));
        }

        job.waitForCompletion(true);

        if (!job.isSuccessful()) {
            throw new Exception("Task finished unsuccessfully");
        }
    }
}