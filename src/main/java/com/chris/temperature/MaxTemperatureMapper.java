package com.chris.temperature;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MaxTemperatureMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Maps a single input key/value pair into an intermediate key/value pair.
     *
     * <p>Output pairs need not be of the same types as input pairs.  A given
     * input pair may map to zero or many output pairs.  Output pairs are
     * collected with calls to
     * {@link OutputCollector#collect(Object, Object)}.</p>
     *
     * <p>Applications can use the {@link Reporter} provided to report progress
     * or just indicate that they are alive. In scenarios where the application
     * takes significant amount of time to process individual key/value
     * pairs, this is crucial since the framework might assume that the task has
     * timed-out and kill that task. The other way of avoiding this is to set
     * <a href="{@docRoot}/../mapred-default.html#mapreduce.task.timeout">
     * mapreduce.task.timeout</a> to a high-enough value (or even zero for no
     * time-outs).</p>
     *
     * @param key      the input key.
     * @param value    the input value.
     * @param output   collects mapped keys and values.
     * @param reporter facility to report progress.
     */
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//            public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemperature = Integer.parseInt(line.substring(87, 92));
        output.collect(new Text(year), new IntWritable(airTemperature));
//        context.write(new Text(year), new IntWritable(airTemperature));
    }

    /**
     * Initializes a new instance from a {@link JobConf}.
     *
     * @param job the configuration
     */
    @Override
    public void configure(JobConf job) {

    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }
}
