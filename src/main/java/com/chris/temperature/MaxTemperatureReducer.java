package com.chris.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
//        while(values.hasNext()){
        for(IntWritable value : values){
//            IntWritable value = (IntWritable)values.next();
            maxValue = Math.max(maxValue, value.get());
        }

        context.write(key, new IntWritable(maxValue));
    }
}
