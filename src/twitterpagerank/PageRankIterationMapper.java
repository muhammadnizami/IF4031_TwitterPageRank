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
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author nizami
 */
public class PageRankIterationMapper extends Mapper<Text, Text, Text, PageRankFollower>{
    
    @Override
    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
        PageRankFollower follower = new PageRankFollower();
        follower.readString(value.toString());
        if (follower.getFolloweeString() != null)
            key = follower.getFollowee();
        context.write(key, follower);
    }
}
