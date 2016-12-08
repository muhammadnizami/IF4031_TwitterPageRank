/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author nizami
 */
public class PageRankBeforeIterationMapper extends Mapper<Text, Text, Text, PageRankFollower>{
    
    @Override
    public void map(Text key, Text value, Mapper.Context context
                    ) throws IOException, InterruptedException {
        String valuestr = value.toString();
        PageRankFollower pageRankFollower = new PageRankFollower();
        boolean isPageRankFollowerObject = pageRankFollower.readString(valuestr);
        if (!isPageRankFollowerObject){
            //key-value: USER, follower
            pageRankFollower.setFollowee(key.toString());
            pageRankFollower.setFollower(valuestr);
            pageRankFollower.setN(-1);
            pageRankFollower.setPageRank(-1);
            context.write(value, pageRankFollower);
        }else{
            context.write(key, pageRankFollower);
        }
    }
    
}
