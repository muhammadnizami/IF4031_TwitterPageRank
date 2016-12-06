/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author nizami
 */
public class PageRankTop5Mapper extends Mapper<Text,Text,NullWritable,SortedPageRank> {
    
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        PageRankFollower pageRankFollower = new PageRankFollower();
        pageRankFollower.readString(value.toString());
        
        SortedPageRank rvalue = new SortedPageRank();
        rvalue.add(pageRankFollower.getFollowerString(),pageRankFollower.getPageRankDouble());
        
        context.write(NullWritable.get(), rvalue);
    }
    
}
