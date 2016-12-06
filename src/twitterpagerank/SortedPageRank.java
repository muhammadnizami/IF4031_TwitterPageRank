/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author nizami
 */
public class SortedPageRank implements WritableComparable<SortedPageRank>{

    @Override
    public void write(DataOutput d) throws IOException {
        IntWritable size = new IntWritable(priorityQueue.size());
        size.write(d);
        PageRankItem [] a = new PageRankItem[priorityQueue.size()];
        for (PageRankItem e : priorityQueue.toArray(a)){
            e.write(d);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        IntWritable size = new IntWritable();
        size.readFields(di);
        priorityQueue = new PriorityQueue<PageRankItem>();
        for (int i=0;i<size.get();i++){
            PageRankItem item = new PageRankItem();
            item.readFields(di);
            priorityQueue.add(item);
        }
    }

    @Override
    public int compareTo(SortedPageRank o) {
        return new Integer(size()).compareTo(new Integer(o.size()));
    }

    public static class PageRankItem implements Comparable<PageRankItem>, WritableComparable<PageRankItem>{
        public String id;
        public Double rank;
        
        public PageRankItem(String id, Double rank){
            this.id = id;
            this.rank = rank;
        }

        private PageRankItem() {
            
        }

        @Override
        public int compareTo(PageRankItem o) {
            return rank.compareTo(o.rank);
        }
        
        public String toString(){
            return id + "\t" + rank;
        }

        @Override
        public void write(DataOutput d) throws IOException {
            (new Text(id)).write(d);
            (new DoubleWritable(rank)).write(d);
            }

        @Override
        public void readFields(DataInput di) throws IOException {
            Text idText = new Text();
            idText.readFields(di);
            DoubleWritable rankWritable = new DoubleWritable();
            rankWritable.readFields(di);
            id = idText.toString();
            rank = rankWritable.get();
        }
        
    }
    
    PriorityQueue<PageRankItem> priorityQueue = new PriorityQueue<>();
    
    public void add(String id, double rank){
        add(id, new Double(rank));
    }
    public void add(String id, Double rank){
        priorityQueue.add(new PageRankItem(id,rank));
    }
    public boolean exists (String id){
        PageRankItem [] a = new PageRankItem[priorityQueue.size()];
        for (PageRankItem e : priorityQueue.toArray(a)){
            if (e.id.equals(id))
                return true;
        }
        return false;
    }
    public void cut(int n){
        while (priorityQueue.size()>n)
            priorityQueue.remove();
    }
    public String toString(){
        String ret = "";
        PageRankItem [] pageRankItems = new PageRankItem[priorityQueue.size()];
        pageRankItems = priorityQueue.toArray(pageRankItems);
        for (PageRankItem item : pageRankItems){
            ret+=item+"\n";
        }
        return ret;
    }
    public int size(){
        return priorityQueue.size();
    }
    public boolean isEmpty(){
        return priorityQueue.isEmpty();
    }
    public void moveAllElementsTo(SortedPageRank dst){
        
        while (!isEmpty()){
            PageRankItem item = priorityQueue.poll();
            dst.priorityQueue.add(item);
        }
    }
}
