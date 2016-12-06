/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nizami
 */
public class TestReadMapper extends Mapper<Text,Text,Text,Text>{
    public void reduce(Text key, Text value,
                       Reducer.Context context
                       ) throws IOException, InterruptedException {
        context.write(new Text("key:"+key.toString()), new Text("value:"+value.toString()));
    }
}
