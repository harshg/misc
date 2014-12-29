import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/*
 * @author Harsh Goel
 * Partitions the input keys into the sections defined by the splitters defined in "splitters.txt"
 * "splitters.txt" is a distributed cache file and must loaded in every node before execution.
 */
public class MrSortPartitioner implements Partitioner<Text,Text> {

	private ArrayList<Text> splitters; //list of splitters
	private HashMap<String, Integer> keyMap; //maps characters to index in the list, for faster lookup
	private final String splittersCacheName = "splitters.txt"; //name of file on local system

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
	 */
	public void configure(JobConf conf) {
		 try {
		      Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		      if (null != cacheFiles && cacheFiles.length > 0) {
		        for (Path cachePath : cacheFiles) {
		          if (cachePath.getName().equals(splittersCacheName)) {
		            loadSplitters(cachePath);
		            break;
		          }
		        }
		      }
		    } 
		 	catch (IOException ioe) {
		      System.err.println("IOException reading from distributed cache");
		      System.err.println(ioe.toString());
		    }
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	public int getPartition(Text key, Text value, int numPartitions) {
		return getSplitNumber(key);
	}
	
   /*
    * Takes the file containing splitters and creates a splitter arrayList and a key Map
    * note use of regular java.io methods here - this is a local file now with the given cachePath
    */
   private void loadSplitters(Path cachePath) throws IOException {
	   
	    BufferedReader lineReader = new BufferedReader(new FileReader(cachePath.toString()));
	    try {
	      String line;
		  this.splitters = new ArrayList<Text>();
		  this.keyMap = new HashMap<String, Integer>();
	      while ((line = lineReader.readLine()) != null) {
	        this.splitters.add(new Text(line));	        
	        if(!keyMap.containsKey(line.substring(0,1)))//add first occurence of a character to the keyMap
	        		keyMap.put(line.substring(0,1), splitters.size()-1);
	      }
	    } finally {
	      lineReader.close();
	    }
   }
	
	/*
	 * Returns the split that this line belongs to.
	 * If there ate 5 splitters, it will return 0 through 5
	 *  Area0<Splitter0>Area1<Splitter1>Area2<Splitter2>Area3<Splitter3>Area4<Splitter4>Area5
	 */
	public int getSplitNumber(Text line)
	{
		//if the key is mapped in our hashmap, narrow our search
		if(keyMap.containsKey(line.toString().substring(0,1))){
			int startIndex = keyMap.get(line.toString().substring(0,1));
			int endIndex = startIndex; //last index in the list with the same key
			while((endIndex+1)<splitters.size() && splitters.get(endIndex+1).toString().substring(0,1).equals(line.toString().substring(0,1)))
					endIndex++;
			return searchSplitterArray(startIndex, endIndex, line);	
		}
		else{
			return searchSplitterArray(0, splitters.size()-1,line);//search through all splitters
		}
	}
	
	/*
	 * Searches the splitter array in the passed indexes and returns the correct split number.
	 */
	private int searchSplitterArray(int startIndex, int endIndex, Text line)
	{
		//edge case splitters
		if(line.toString().compareTo(splitters.get(startIndex).toString())<0)
			return startIndex;
		else if(line.toString().compareTo(splitters.get(endIndex).toString())>=0)
			return endIndex+1;
		
		for(int i=startIndex+1;i<=endIndex;i++)
		{
			if(line.toString().compareTo(splitters.get(i).toString())<0)
				return i;
		}
		return -1; //error
	}

}
