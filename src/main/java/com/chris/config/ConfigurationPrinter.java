package com.chris.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.lang.model.SourceVersion;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

public class ConfigurationPrinter extends Configured implements Tool {

//    static{
//        Configuration.addDefaultResource("hadoop-local.xml");
//        Configuration.addDefaultResource("hadoop-localhost.xml");
//        Configuration.addDefaultResource("hadoop-clusterone.xml");
//    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        for(Map.Entry<String, String> entry : conf){
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args)throws Exception{
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }
}
