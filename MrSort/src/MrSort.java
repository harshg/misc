import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;

/*
 * @author Harsh Goel
 * This implementation of MapReduce sorts standard sort files of any size
 * Plan:
 * 1. look at files randomly and find splitters
 * 2. map - emit each value
 * 3. Set up a partitioning function to split to different reducers
 * 4. reducer - send value to the output, inherently sorts values.
 * 
 */

public class MrSort {

  private static final int numSplits = 300;  //~60 worker machines in cluster * 2.5, for efficient load balancing
  private static final int numSamplesPerSplitter = 10; //can be varied
  
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(MrSort.class);

    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    
   //specify input type
    conf.setInputFormat(KeyValueTextInputFormat.class); //input is <out line, blank string>
    
    //configure our job
    conf.setNumMapTasks(3125); //Optional:change split size. generally it is 64MB, good for large files, mentioned in mapreduce paper by google
     conf.setNumReduceTasks(numSplits); 
    
    // specify input and output dirs
    FileInputFormat.addInputPath(conf, new Path("/home/cct-tirthapura/input"));
    FileOutputFormat.setOutputPath(conf, new Path("/home/cct-tirthapura/output"));

    
    //find splitters to partition input
    try {
    	//get random samples from the input
    	Object sampleKeys[];
        InputSampler.RandomSampler<Text,Text> sampler = new InputSampler.RandomSampler<Text,Text>(0.1, numSplits*numSamplesPerSplitter, numSamplesPerSplitter); 
		sampleKeys = sampler.getSample((KeyValueTextInputFormat)conf.getInputFormat(), conf);
		Arrays.sort(sampleKeys);
		
		//write splitters to a local file
		final String localPath = "/home/cct-tirthapura/splitters.txt";
		FileWriter fstream = new FileWriter(localPath);
		PrintWriter fwrite = new PrintWriter(fstream);
		for(int i=1;i<=(numSplits-1);i++)
		{
			fwrite.println(sampleKeys[i*numSamplesPerSplitter-1]);
		}
		fwrite.close();
		
		//send local file to distributed cache
		final String hdfsPath = "/home/cct-tirthapura/extra/splitters.txt";
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(true, true, new Path(localPath), new Path(hdfsPath));
		DistributedCache.addCacheFile(new Path(hdfsPath).toUri(), conf);
		
	} catch (IOException e1) {
		e1.printStackTrace();
		return;
	}
	
	//specify a partitioner
    conf.setPartitionerClass(MrSortPartitioner.class);
    
    // specify a mapper
   conf.setMapperClass(MrSortMapper.class);

    // specify a reducer
    conf.setReducerClass(MrSortReducer.class);
    conf.setCombinerClass(MrSortReducer.class);

    client.setConf(conf);
    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}