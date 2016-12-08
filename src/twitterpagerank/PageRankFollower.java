/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author nizami
 */
public class PageRankFollower implements WritableComparable<PageRankFollower>{
    
    private Text follower;
    private DoubleWritable pageRank;
    private LongWritable n;
    private Text followee;
    
    public PageRankFollower(){
        follower = new Text("null");
        n = new LongWritable(-1);
        followee = new Text("null");
        pageRank = new DoubleWritable(-1);
    }
    
    public PageRankFollower(String follower,double pageRank, long n, String followee){
        this.follower = new Text(follower==null?"null":follower);
        this.n = new LongWritable(n);
        this.followee = new Text(followee==null?"null":follower);
        this.pageRank = new DoubleWritable(pageRank);
    }
    
    public PageRankFollower(Text follower,double pageRank, long n, Text followees){
        this.follower = follower;
        this.n = new LongWritable(n);
        this.followee = followees;
        this.pageRank = new DoubleWritable(pageRank);
    }
    
    public PageRankFollower(PageRankFollower o){
        setFollower(o.getFollowerString());
        setFollowee(o.getFolloweeString());
        setN(o.getNlong());
        setPageRank(o.getPageRankDouble());
    }

    @Override
    public void write(DataOutput d) throws IOException {
        follower.write(d);
        pageRank.write(d);
        n.write(d);
        followee.write(d);
    }
    
    public String toString(){
        String rval=
                follower.toString()+","+
                pageRank.toString()+","+
                n.toString()+","+
                followee.toString();
        return rval;
                
    }
    
    public boolean readString(String s){
        String [] fields = s.split(",");
        if (fields.length<4)
            return false;
        pageRank = new DoubleWritable(Double.parseDouble(fields[1]));
        n = new LongWritable(Long.parseLong(fields[2]));
        followee = new Text(fields[3]);
        follower = new Text(fields[0]);
        return true;
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        follower.readFields(di);
        pageRank.readFields(di);
        n.readFields(di);
        followee.readFields(di);
    }

    @Override
    public int compareTo(PageRankFollower o) {
        if (follower.compareTo(o.follower)==0){
            if (n.compareTo(o.n)==0){
                return pageRank.compareTo(pageRank);
            }else{
                return n.compareTo(o.n);
            }
        }else{
            return follower.compareTo(o.follower);
        }
    }

    /**
     * @return the follower
     */
    public Text getFollower() {
        return follower;
    }

    /**
     * @param follower the follower to set
     */
    public void setFollower(Text follower) {
        this.follower = follower;
    }

    /**
     * @return the n
     */
    public LongWritable getN() {
        return n;
    }

    /**
     * @param n the n to set
     */
    public void setN(LongWritable n) {
        this.n = n;
    }

    /**
     * @return the pageRank
     */
    public DoubleWritable getPageRank() {
        return pageRank;
    }

    /**
     * @param pageRank the pageRank to set
     */
    public void setPageRank(DoubleWritable pageRank) {
        this.pageRank = pageRank;
    }
    

    /**
     * @return the follower
     */
    public String getFollowerString() {
        String followerstr = follower.toString();
        
        return followerstr.equals("null")?null:followerstr;
    }
    

    /**
     * @return the follower
     */
    public String getFolloweeString() {
        String followeestr = followee.toString();
        
        return followeestr.equals("null")?null:followeestr;
    }

    /**
     * @param follower the follower to set
     */
    public void setFollower(String follower) {
        this.follower = new Text(follower);
    }

    /**
     * @return the n
     */
    public long getNlong() {
        return n.get();
    }

    /**
     * @param n the n to set
     */
    public void setN(long n) {
        this.n = new LongWritable(n);
    }

    /**
     * @return the pageRank
     */
    public double getPageRankDouble() {
        return pageRank.get();
    }

    /**
     * @param pageRank the pageRank to set
     */
    public void setPageRank(double pageRank) {
        this.pageRank = new DoubleWritable(pageRank);
    }
    
    public void combineFollowees(PageRankFollower p2){
        this.setFollower(p2.getFollower());
        this.setPageRank(p2.getPageRankDouble());
        this.setN(this.getNlong()+p2.getNlong());
        this.setFollowee((String)null);
    }

    /**
     * @return the followees
     */
    public Text getFollowee() {
        return followee;
    }

    /**
     * @param followees the followees to set
     */
    public void setFollowee(Text followee) {
        this.followee = followee;
    }
    
    public void setFollowee(String followee){
        this.followee = new Text(followee==null?"null":followee);
    }
}
