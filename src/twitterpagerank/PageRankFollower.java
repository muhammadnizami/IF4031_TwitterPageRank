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
    private List<Text> followees;
    
    public PageRankFollower(){
        follower = new Text();
        n = new LongWritable();
        followees = new LinkedList<Text>();
        pageRank = new DoubleWritable();
    }
    
    public PageRankFollower(String follower,double pageRank, long n, List<Text> followees){
        this.follower = new Text(follower);
        this.n = new LongWritable(n);
        this.followees = followees;
        this.pageRank = new DoubleWritable(pageRank);
    }
    
    public PageRankFollower(Text follower,double pageRank, long n, List<Text> followees){
        this.follower = follower;
        this.n = new LongWritable(n);
        this.followees = followees;
        this.pageRank = new DoubleWritable(pageRank);
    }

    @Override
    public void write(DataOutput d) throws IOException {
        follower.write(d);
        pageRank.write(d);
        n.write(d);
        new LongWritable(getFollowees().size()).write(d);
        for (Text i : getFollowees()){
            i.write(d);
        }
    }
    
    public String toString(){
        String rval=
                follower.toString()+","+
                pageRank.toString()+","+
                n.toString()+","+
                getFollowees().size()+",";
        for (Text i : getFollowees()){
            rval+=i.toString()+",";
        }
        return rval;
                
    }
    
    public void readString(String s){
        String [] fields = s.split(",");
        pageRank = new DoubleWritable(Double.parseDouble(fields[1]));
        n = new LongWritable(Long.parseLong(fields[2]));
        int followeesSize = Integer.parseInt(fields[3],10);
        setFollowees(new ArrayList<Text>(followeesSize));
        for (int i = 0;i<followeesSize;i++){
            getFollowees().add(new Text(fields[i+4]));
        }
        follower = new Text(fields[0]);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        follower.readFields(di);
        pageRank.readFields(di);
        n.readFields(di);
        LongWritable followeesSizeWritable =  new LongWritable();
        followeesSizeWritable.readFields(di);
        long followeesSize = followeesSizeWritable.get();
        followees = new ArrayList<Text>((int) followeesSize);
        for (long i=0;i<followeesSize;i++){
            Text followee = new Text();
            followee.readFields(di);
            getFollowees().add(followee);
        }
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
        return follower.toString();
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
        this.getFollowees().addAll(p2.getFollowees());
    }

    /**
     * @return the followees
     */
    public List<Text> getFollowees() {
        return followees;
    }

    /**
     * @param followees the followees to set
     */
    public void setFollowees(List<Text> followees) {
        this.followees = followees;
    }
}
