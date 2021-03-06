/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nizami
 */
public class PageRankIterationReducer extends Reducer<Text,PageRankFollower,Text,PageRankFollower>{
    
    double d = 0.85;
    
    public PageRankFollower calculatePageRank(Text key, Iterable<PageRankFollower> values){
        
        PageRankFollower result = new PageRankFollower(key.toString(), -1, -1, null);
        double newRank = 0;
        for (PageRankFollower p : values) {
            if (p.getFollower().toString().equals(key.toString()+"c")){
                newRank += p.getPageRankDouble();
                if (p.getNlong()>=0)
                    result.setN(p.getNlong());
            }else if (p.getFollower().toString().equals(key.toString())){
                newRank += (1.0-d)*(p.getPageRankDouble());
                if (p.getNlong()>=0)
                    result.setN(p.getNlong());
            }else{
                newRank += (d*p.getPageRankDouble())/p.getNlong();
            }
        }
        result.setPageRank(newRank);
        return result;
    }
    
    @Override
    public void reduce(Text key, Iterable<PageRankFollower> values, Context context) throws IOException, InterruptedException {
        PageRankFollower result = calculatePageRank(key,values);
        context.write(key, result);
    }
    
    public static class PageRankIteratorCombiner extends PageRankIterationReducer{
        @Override
        public PageRankFollower calculatePageRank(Text key, Iterable<PageRankFollower> values){
            PageRankFollower result = super.calculatePageRank(key, values);
            result.setFollower(result.getFollowerString()+"c");
            return result;
        }
    }
}
