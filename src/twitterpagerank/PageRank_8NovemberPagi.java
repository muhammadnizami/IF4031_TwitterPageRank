/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterpagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author nizami
 */
public class PageRank_8NovemberPagi {
    
    public static int numIter = 3;
    
    public static void main(String[] args) throws Exception {
        String jobsPrefix = "nizami - pageRank - "+args[0]+ " - ";
        String initializationFilePath = "iteration0";
            
            
            Configuration conf4 = new Configuration();
            conf4.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            Job iterationJob_ = Job.getInstance(conf4, jobsPrefix+"iteration"+3);
            iterationJob_.setJarByClass(PageRank_8NovemberPagi.class);
            iterationJob_.setMapperClass(PageRankIterationMapper.class);
            iterationJob_.setCombinerClass(PageRankIterationReducer.PageRankIteratorCombiner.class);
            iterationJob_.setReducerClass(PageRankIterationReducer.class);
            iterationJob_.setInputFormatClass(KeyValueTextInputFormat.class);
            iterationJob_.setMapOutputKeyClass(Text.class);
            iterationJob_.setMapOutputValueClass(PageRankFollower.class);
            iterationJob_.setOutputKeyClass(Text.class);
            iterationJob_.setOutputValueClass(PageRankFollower.class);
            TextInputFormat.addInputPath(iterationJob_, new Path(args[1]+"/beforeiteration"+3));
            TextOutputFormat.setOutputPath(iterationJob_, new Path(args[1]+"/iteration"+3));

            iterationJob_.waitForCompletion(true);
        
        for (int i=4;i<=numIter;i++){
            
            
            Configuration conf3 = new Configuration();
            conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            Job beforeIterationJob = Job.getInstance(conf3, jobsPrefix+"before_iteration"+i);
            beforeIterationJob.setJarByClass(PageRank_8NovemberPagi.class);
            beforeIterationJob.setMapperClass(PageRankBeforeIterationMapper.class);
            beforeIterationJob.setCombinerClass(PageRankBeforeIterationReducer.class);
            beforeIterationJob.setReducerClass(PageRankBeforeIterationReducer.class);
            beforeIterationJob.setInputFormatClass(KeyValueTextInputFormat.class);
            beforeIterationJob.setMapOutputKeyClass(Text.class);
            beforeIterationJob.setMapOutputValueClass(PageRankFollower.class);
            beforeIterationJob.setOutputKeyClass(Text.class);
            beforeIterationJob.setOutputValueClass(PageRankFollower.class);
            TextInputFormat.addInputPath(beforeIterationJob, new Path(args[1]+"/iteration"+(i-1)));
            TextInputFormat.addInputPath(beforeIterationJob, new Path(args[0]));
            TextOutputFormat.setOutputPath(beforeIterationJob, new Path(args[1]+"/beforeiteration"+i));

            beforeIterationJob.waitForCompletion(true);
            
            
            Configuration conf2 = new Configuration();
            conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            Job iterationJob = Job.getInstance(conf2, jobsPrefix+"iteration"+i);
            iterationJob.setJarByClass(PageRank_8NovemberPagi.class);
            iterationJob.setMapperClass(PageRankIterationMapper.class);
            iterationJob.setCombinerClass(PageRankIterationReducer.PageRankIteratorCombiner.class);
            iterationJob.setReducerClass(PageRankIterationReducer.class);
            iterationJob.setInputFormatClass(KeyValueTextInputFormat.class);
            iterationJob.setMapOutputKeyClass(Text.class);
            iterationJob.setMapOutputValueClass(PageRankFollower.class);
            iterationJob.setOutputKeyClass(Text.class);
            iterationJob.setOutputValueClass(PageRankFollower.class);
            TextInputFormat.addInputPath(iterationJob, new Path(args[1]+"/beforeiteration"+i));
            TextOutputFormat.setOutputPath(iterationJob, new Path(args[1]+"/iteration"+i));

            iterationJob.waitForCompletion(true);
        }
        
        Configuration conf3 = new Configuration();
        conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job finalizationJob = Job.getInstance(conf3, jobsPrefix+"finalization");
        finalizationJob.setJarByClass(PageRank.class);
        finalizationJob.setMapperClass(PageRankTop5Mapper.class);
        finalizationJob.setCombinerClass(PageRankTop5Reducer.class);
        finalizationJob.setReducerClass(PageRankTop5Reducer.class);
        finalizationJob.setInputFormatClass(KeyValueTextInputFormat.class);
        finalizationJob.setMapOutputKeyClass(NullWritable.class);
        finalizationJob.setMapOutputValueClass(SortedPageRank.class);
        finalizationJob.setOutputKeyClass(NullWritable.class);
        finalizationJob.setOutputValueClass(SortedPageRank.class);
        TextInputFormat.addInputPath(finalizationJob, new Path(args[1]+"/iteration"+(numIter)));
        TextOutputFormat.setOutputPath(finalizationJob, new Path(args[1]+"/final"));

        finalizationJob.waitForCompletion(true);
    }
    
}
