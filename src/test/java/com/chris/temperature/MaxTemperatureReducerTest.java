package com.chris.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class MaxTemperatureReducerTest {

    @Test
    public void returnsMaxmumIntegerInValues() throws IOException, InterruptedException {
        ReduceDriver reduceDriver = new ReduceDriver<Text, IntWritable, Text,IntWritable>();
        Reducer reducer = new MaxTemperatureReducer();
        reduceDriver.withReducer(reducer)
                .withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }
}