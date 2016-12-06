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
public class PageRank {
    
    public static int numIter = 3;
    
    public static void main(String[] args) throws Exception {
        String jobsPrefix = "nizami - pageRank - "+args[0]+ " - ";
        String initializationFilePath = "iteration0";
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");//TODO sesuaikan separator dengan di spek
        Job initializationJob = Job.getInstance(conf, jobsPrefix+"initialization");
        initializationJob.setJarByClass(PageRank.class);
        initializationJob.setMapperClass(PageRankInitializerMapper.class);
        initializationJob.setCombinerClass(PageRankInitializerReducer.class);
        initializationJob.setReducerClass(PageRankInitializerReducer.class);
        initializationJob.setInputFormatClass(KeyValueTextInputFormat.class);
        initializationJob.setMapOutputKeyClass(Text.class);
        initializationJob.setMapOutputValueClass(PageRankFollower.class);
        initializationJob.setOutputKeyClass(Text.class);
        initializationJob.setOutputValueClass(PageRankFollower.class);
        TextInputFormat.addInputPath(initializationJob, new Path(args[0]));
        TextOutputFormat.setOutputPath(initializationJob, new Path(args[1]+"/"+initializationFilePath));
        initializationJob.waitForCompletion(true);
        
        for (int i=1;i<=numIter;i++){
            Configuration conf2 = new Configuration();
            conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            Job iterationJob = Job.getInstance(conf2, jobsPrefix+"iteration"+i);
            iterationJob.setJarByClass(PageRank.class);
            iterationJob.setMapperClass(PageRankIterationMapper.class);
            iterationJob.setCombinerClass(PageRankIterationReducer.PageRankIteratorCombiner.class);
            iterationJob.setReducerClass(PageRankIterationReducer.class);
            iterationJob.setInputFormatClass(KeyValueTextInputFormat.class);
            iterationJob.setMapOutputKeyClass(Text.class);
            iterationJob.setMapOutputValueClass(PageRankFollower.class);
            iterationJob.setOutputKeyClass(Text.class);
            iterationJob.setOutputValueClass(PageRankFollower.class);
            TextInputFormat.addInputPath(iterationJob, new Path(args[1]+"/iteration"+(i-1)));
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
