package edu.stanford.cs246.PeopleYouMayKnow;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PeopleYouMayKnow {
	 public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Configuration conf1 = new Configuration();
	      
	      Job job1 = new Job(conf1, "PeopleYouMayKnow-MR1");
	      job1.setJarByClass(PeopleYouMayKnow.class);
	      job1.setOutputKeyClass(Text.class);
	      job1.setOutputValueClass(IntWritable.class);

	      job1.setMapperClass(MapPossibleFriends.class);
	      job1.setReducerClass(ReducePossibleFriends.class);

	      job1.setInputFormatClass(TextInputFormat.class);
	      job1.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job1, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job1, new Path(args[1]));

	      job1.waitForCompletion(true);
	      
	      //run 2nd MR job
	      Configuration conf2 = new Configuration();
	      
	      Job job2 = new Job(conf2, "PeopleYouMayKnow-MR2");
	      job2.setJarByClass(PeopleYouMayKnow.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Text.class);

	      job2.setMapperClass(MapLikelyFriends.class);
	      job2.setReducerClass(ReduceLikelyFriends.class);

	      job2.setInputFormatClass(TextInputFormat.class);
	      job2.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job2, new Path(args[2]));
	      FileOutputFormat.setOutputPath(job2, new Path(args[3]));

	      job2.waitForCompletion(true);
	   }
	   
	   public static class MapPossibleFriends extends Mapper<LongWritable, Text, Text, IntWritable> {
	      private final static IntWritable ZERO = new IntWritable(0);
	      private final static IntWritable ONE = new IntWritable(1);

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	 String[] line = value.toString().split("\t");
	         //make sure there's actually a uid, friend line to work with
	    	 if (line.length == 2){
	    		 String uid = line[0];
	        	 String[] friends = line[1].split(",");
		         int num_friends = friends.length;
	    		 //make (k,v) pairs; for k = (uid, friends[i]), v = 0
		         //for k = (friends[i], friends[j]), v = 1
		         for(String item : friends){
		        	 context.write(new Text(uid + ',' + item), ZERO);
		         }
		         for(int i = 0; i < num_friends; i++){
		        	 for(int j = i + 1; j < num_friends; j++){
		        		 context.write(new Text(friends[i] + ',' + friends[j]), ONE);
		        	 }
		         }
	    	 }
	    	 else
	    		 return;
	      }
	   }
	   
	   public static class ReducePossibleFriends extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	         /*sum each key to get total friend in common possibilities
	    	 want to remove key that may have friends in common, but
	    	 are already friends so if any values are zero for a given key,
	    	 remove that key
	    	 */
	    	 int sum = 0;
	    	 int product = 1;
	         for (IntWritable val : values) {
	            sum += val.get();
	            product *= val.get();
	         }
	         if (product != 0)
	        	context.write(key, new IntWritable(sum));
	      }
	   }

	   public static class MapLikelyFriends extends Mapper<LongWritable, Text, Text, Text> {
		   @Override
		   public void map(LongWritable key, Text value, Context context)
		           throws IOException, InterruptedException {
			 /*
			  * We now read in the file that contains the results of the 
			  * 1st MR job.
			  * Each line has format: 'uid,friend_id\tnum_2nd_connections'
			  * This map method maps key to (uid, num_2nd_connections) and 
			  * value to friend_id
			  */
			 //could using indexing and substrings here instead
			 String[] uid_fidnum = value.toString().split(",");
		  	 String[] fid_num = uid_fidnum[1].split("\t");
		  	 context.write(new Text(uid_fidnum[0] + ',' + fid_num[1]), new Text(fid_num[0]));
		   }
	   }
	   
	   public static class ReduceLikelyFriends extends Reducer<Text, Text, Text, Text> {
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	      /*
	       * TODO: sort friends in ascending order for each key, taking only <= top 10 friend
	       * put into 2D array for each uid with each row being ascending order of friends for
	       * a given number of friends in common.
	       * Make csv string of 10 or less friends using friends using row with highest number of
	       * friends in common first then working down until string has 10 friends or no more friends
	       * left.
	       * Emit key as uid and value as csv string
	       */
	    	  
	      }
	   }
}
	   
		 
