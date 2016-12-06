
import junit.framework.Assert;
import junit.framework.TestCase;
import twitterpagerank.SortedPageRank;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author nizami
 */
public class SortedPageRankTest extends TestCase {
    public void test1(){
        SortedPageRank sortedPageRank = new SortedPageRank();
        sortedPageRank.add("a", 1.0);
        sortedPageRank.add("b", 2.0);
        sortedPageRank.add("c", 3.0);
        sortedPageRank.add("0", 0.0);
        System.out.println("before cut: \n"+sortedPageRank);
        sortedPageRank.cut(2);
        System.out.println("after cut: \n" + sortedPageRank);
        System.out.println("size: " + sortedPageRank.size());
        Assert.assertTrue(sortedPageRank.exists("b"));
        Assert.assertTrue(sortedPageRank.exists("c"));
        Assert.assertFalse(sortedPageRank.exists("0"));
        Assert.assertFalse(sortedPageRank.exists("a"));
    }
}
