/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nizami
 */
public class PageRankBeforeIterationReducer extends Reducer<Text,PageRankFollower, Text, PageRankFollower>{
    
    @Override
    public void reduce(Text key, Iterable<PageRankFollower> values, Context context) throws IOException, InterruptedException {
        double PageRank = -1;
        long n = -1;
        List<PageRankFollower> values2 = new LinkedList<PageRankFollower>();
        for (PageRankFollower value : values){
            if (value.getPageRankDouble() >=0){
                PageRank = value.getPageRankDouble();
            }
            if (value.getNlong() >= 0){
                n=value.getNlong();
            }
            values2.add(new PageRankFollower(value));
        }
        for (PageRankFollower value : values2){
            value.setN(n);
            value.setPageRank(PageRank);
            context.write(key, value);
        }
    }
    
}
