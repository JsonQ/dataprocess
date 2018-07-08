package com.chris.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class MaxTemperatureMapperTest {

    private String record = "0043011990999991950051518004+68750+023550FM-12+0382"
            + "99999V0203201n00261220001CN9999999N9-00111+99999999999";
    private Text value;

    @Before
    public void before() {
        value = new Text(record);
    }

    @Test
    public void processValidRecord() throws IOException, InterruptedException {
        Text value = new Text(record);

        MapDriver mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        Mapper mapper = new MaxTemperatureMapperV1();
        mapDriver.withMapper(mapper)
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {

        MapDriver mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.withMapper(new MaxTemperatureMapperV1())
                .withInput(new LongWritable(0), value)
                .runTest();
    }
}