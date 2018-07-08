package com.chris.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;



public class MaxTemperatureMapperV1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaxTemperatureMapperV1.class);

    private NcdcRecordParser parse = new NcdcRecordParser();

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parse.parse(value);
        if(parse.isValidTemperature()){
            LOGGER.debug("Temperature clear: " + parse.getYear() + "-" + parse.getAirTemperature());
            context.write(new Text(parse.getYear()), new IntWritable(parse.getAirTemperature()));
        }
    }
}
