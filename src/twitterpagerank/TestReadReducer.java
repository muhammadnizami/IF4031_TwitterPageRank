/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nizami
 */
public class TestReadReducer extends Reducer<Text,Text,Text,Text>{
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Reducer.Context context
                       ) throws IOException, InterruptedException {
        String valuestr = "wololo";
        for (Text t : values){
            valuestr+=t.toString();
        }
        result.set(valuestr);
        context.write(key, result);
    }
}
