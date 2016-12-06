
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import twitterpagerank.PageRankFollower;
import twitterpagerank.PageRankInitializerReducer;
import twitterpagerank.PageRankIterationReducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author nizami
 */
public class PageRankIterationReducerTest extends TestCase{
    
    public void test1(){
        Text key = new Text("testKey");
        PageRankFollower pageRankFollower = new PageRankFollower(new Text(key.toString()), 1.0, 0, new ArrayList<Text>());
        List<PageRankFollower> values = new ArrayList<PageRankFollower>();
        values.add(pageRankFollower);
        
        PageRankFollower result = (new PageRankIterationReducer()).calculatePageRank(key, values);
        System.out.println("result: " + result);
        
        
    }
    
}
