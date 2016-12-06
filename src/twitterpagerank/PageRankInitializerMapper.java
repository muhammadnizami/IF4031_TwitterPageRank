/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author nizami
 */
public class PageRankInitializerMapper extends Mapper<Text, Text, Text, PageRankFollower>{
    
    double initialPageRank = 1.0;
    
    @Override
    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
        Text followee = key;
        Text follower = value;
        List<Text> followees = new ArrayList<Text>(1);
        followees.add(followee);
        PageRankFollower pageRankFollower = new PageRankFollower(follower,initialPageRank,1,followees);
        context.write(follower, pageRankFollower);
        
        PageRankFollower pageRankFollower2 = new PageRankFollower(followee,initialPageRank,0,new ArrayList<Text>());
        context.write(followee, pageRankFollower2);
        
    }
}
