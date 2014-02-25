package com.lidl.hadoop;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        final Configuration conf = new Configuration();
        /**
         * 
         final String basePath="/Users/lidl/Documents/tmp/hadoopconf/"; conf.addResource(new
         * Path(basePath+"hdfs-site.xml")); conf.addResource(new Path(basePath+ "mapred-site.xml"));
         * conf.addResource(new Path(basePath+"core-site.xml"));
         * conf.set("yarn.resourcemanager.hostname", "192.168.1.100");
         */
        MongoConfigUtil.setInputURI(conf, "mongodb://zonlolo:zonlolo_P2ssW0rd@192.168.1.100/zonlolo.worker");
        MongoConfigUtil
                .setOutputURI(conf, "mongodb://zonlolo:zonlolo_P2ssW0rd@192.168.1.100/zonlolo.out");
        System.out.println("Conf: " + conf);

        final Job job = new Job(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(MongoOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
