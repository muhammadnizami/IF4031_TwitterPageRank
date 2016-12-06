/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nizami
 */
public class PageRankTop5Reducer extends Reducer<NullWritable,SortedPageRank,NullWritable,SortedPageRank> {
    @Override
    public void reduce(NullWritable key, Iterable<SortedPageRank> values, Context context) throws IOException, InterruptedException{
        SortedPageRank result = new SortedPageRank();
        for (SortedPageRank value : values){
            value.moveAllElementsTo(result);
            result.cut(5);
        }
        context.write(NullWritable.get(), result);
    }
}
