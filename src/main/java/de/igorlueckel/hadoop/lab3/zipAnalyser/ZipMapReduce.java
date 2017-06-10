package de.igorlueckel.hadoop.lab3.zipAnalyser;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by igorl on 04.06.2017.
 */
public class ZipMapReduce extends Configured implements Tool {

    private static byte[] hbaseFamily = "data".getBytes();
    private static byte[] hbaseColumn;

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(ZipMapReduce.class);

        String table = "locations";
        hbaseColumn = args[0].getBytes();
        Scan scan = new Scan();
//        if (column != null) {
//            byte[][] columnKey = KeyValue.parseColumn(Bytes.toBytes(column));
//            if (columnKey.length > 1) {
//                scan.addColumn(columnKey[0], columnKey[1]);
//            } else {
//                scan.addFamily(columnKey[0]);
//            }
//        }

        TableMapReduceUtil.initTableMapperJob(table, scan, ZipCounterMapper.class,
                Text.class, IntWritable.class, job);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    private static class ZipCounterMapper extends
            TableMapper<Text, IntWritable> {
        private static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] plz = value.getValue(hbaseFamily, hbaseColumn);
            context.write(new Text(plz), ONE);
        }
    }

    private static class ZipCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
