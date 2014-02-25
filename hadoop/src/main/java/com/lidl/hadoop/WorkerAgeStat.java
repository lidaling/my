package com.lidl.hadoop;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by worker on 2/24/14.
 */
public class WorkerAgeStat {
    private static final Logger logger = LoggerFactory.getLogger(WorkerAgeStat.class);

    public static class TokenzierMapper extends Mapper<Object, BSONObject, Text, IntWritable> {
        private final Logger logger = LoggerFactory.getLogger(TokenzierMapper.class);

        @Override
        protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
            logger.info("key:{},<--->value:{}", new Object[] {key, value.toString()});
            System.out.println("key-value:" + key + "---" + value);
            /**
             * 
             final int year = ((Date) pValue.get("_id")).getYear() + 1900; double bid10Year =
             * ((Number) pValue.get("bc10Year")).doubleValue();
             */
            int grade = ((Number) value.get("grade")).intValue();
            context.write(new Text(String.valueOf(grade)), new IntWritable(1));
        }
    }

    public static class CountAgeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final Logger logger = LoggerFactory.getLogger(CountAgeReducer.class);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int count = 0;
            for (IntWritable intWritable : values) {
                count += intWritable.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();
        conf.setBoolean("mongo.input.split.create_input_splits", false);
        MongoConfigUtil.setInputURI(conf, DBHandler.getCollection("worker"));
        MongoConfigUtil.setOutputURI(conf, DBHandler.getCollection("workerStat"));
        logger.info("conf:{}", conf);

        final Job job = new Job(conf, "worker age stat");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenzierMapper.class);

        job.setReducerClass(CountAgeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
