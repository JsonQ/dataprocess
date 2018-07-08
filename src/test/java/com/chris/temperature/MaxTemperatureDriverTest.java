package com.chris.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class MaxTemperatureDriverTest {

    @Test
    public void test() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("/media/chris/HDD/002_files/tmp/input");
        Path output = new Path("/media/chris/HDD/002_files/tmp/output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[] {input.toString(), output.toString()});
        Assert.assertEquals(exitCode, 0);
//        checkOutput(conf, output);
    }
}
