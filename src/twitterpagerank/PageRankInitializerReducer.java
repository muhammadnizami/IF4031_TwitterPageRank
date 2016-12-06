/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nizami
 */
public class PageRankInitializerReducer extends Reducer<Text, PageRankFollower, Text, PageRankFollower> {
    
    @Override
    public void reduce(Text key, Iterable<PageRankFollower> values, Context context) throws IOException, InterruptedException {
        PageRankFollower result = new PageRankFollower(new Text("0"), 0, 0, new ArrayList<Text>());
        for (PageRankFollower p : values) {
            result.combineFollowees(p);
        }
        context.write(key, result);
    }
    
}
