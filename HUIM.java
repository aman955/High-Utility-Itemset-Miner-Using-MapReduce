
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.MAP;


public class HUIM {
//public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private String T = new String();
        private int Ti = 0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         
       
    StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
 //   System.out.println(profit_table);
    while (itr.hasMoreTokens()) {
    		Ti++;
        T = itr.nextToken().toString();
        String[] T_splite = T.split(",");
        int flag=0;
        
        for(String s : T_splite)
        {
        	 context.write(new Text(Integer.toString(Ti)), new Text(Integer.toString(flag)+":"+Integer.toString(Integer.parseInt(s)*profit_table.get(flag))));
        	 //int num = Integer.parseInt(s)*profit_table.get(flag);
        	 //System.out.println(Integer.toString(Ti)+":"+T+":"+num);
        	 flag++;
        }
        	
       
     

    }
   
        	
  }

}



public static class IntSumReducer

  //   extends Reducer<Text,IntWritable,Text,IntWritable> {
       extends Reducer<Text,Text,Text,Text> {
  private IntWritable result = new IntWritable();
 

  public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

  
   //  int sum=0;
    int i = 0;
    String output = "";
    for (Text val : values) {

    	   String temp = val.toString();
    	   output = temp + " " + output;
    //     if(val.get()>=0)
      //   {
      //  	 max = val.get();
       //  }
       i++;
    }
   String[] orgin_T=new String[i];
   String[] output_splite = output.split(" ");
   int [] num_compare = new int[i];
   for(String s : output_splite) {
	   String[] s_splite = s.split(":");
	   int index = Integer.parseInt(s_splite[0]);
	   orgin_T[index] = s_splite[1];
	   num_compare[index] = Integer.parseInt(s_splite[1]);
   }
  // System.out.println(num_compare);
   /*for(int j=0;j<num_compare.length;j++) {
	   System.out.println(num_compare[j]+" ");
   }*/
  // System.out.println("-------------------");
   int max = num_compare[0];
   for(int j=1;j < num_compare.length;j++){ 
	      if(num_compare[j] > max){ 
	    //	  System.out.println("Now Max is:"+max);
	         max = num_compare[j]; 
	      } 
	    }
  // System.out.println(max);
   String orgin = "";
   for(String o : orgin_T) {
	   if(orgin=="") {
		   orgin = o;
	   }
	   else{
	   orgin = orgin + " " + o;
	   }
   }
   
   
    context.write(new Text(orgin+" "+Integer.toString(max)),null);
   // String[] key_splite = key.toString().split(":");
  //  context.write(key, new IntWritable(max));
   
   // context.write(key, new IntWritable(sum));
    
 
  }
       
 
}
public static class PruneMapper
extends Mapper<Object, Text, Text, IntWritable>{
private String T = new String();
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
 T = itr.nextToken().toString();
 List<Integer> T_value = new ArrayList<Integer>();
  String[] T_splites = T.split(" ");
  for(String s:T_splites) {
	  T_value.add(Integer.parseInt(s));
	  //System.out.println(s);
  }
  
 int max_utility_T = T_value.get(T_value.size()-1);

 for(int i=0;i<T_value.size()-1;i++) {
	 if(T_value.get(i)!=0) {
		 String item_name = Integer.toString(i+1);
		 context.write(new Text(item_name), new IntWritable(max_utility_T));	  
	 }
 }
	 
  
}
}
}
public static class IntSumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
int sum = 0;
int TH=45;
for (IntWritable val : values) {
 sum += val.get();
}

  result.set(sum);
  if(sum>=TH) {
     context.write(key, result);
  }
}
}
public static class LinkMapper 

extends Mapper<Object, Text, Text, Text>{

private String T = new String();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {

T = itr.nextToken().toString();

String[] T_splites = T.split("\t");

if(T_splites[0].length()==1) {
	
	context.write(new Text("one"), new Text(T_splites[0]));
}
else {
	
	 String[] C= T_splites[0].split(",");
	 int C_l = C.length;
	 String prefix = "";
	 for(int i=0;i<C_l-1;i++)
	 {
		 if(prefix=="") {
			 prefix = C[i];
		 }
		 else {
			 prefix = prefix + "," + C[i];
		 }
	 }
	 context.write(new Text(prefix), new Text(C[C_l-1]));
	// System.out.println("Prefix is:"+prefix+" and Link item is:"+C[C_l-1]);
	
	
}



}	




}
}

public static class LinkReducer


extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

/*for (Text val : values) {
context.write(new Text(val.toString()), null);
}*/
	
	
  if(key.toString().contains("one")) {
	 
	
	  List<String> C1 = new ArrayList<String>();
     
    for(Text val : values) {
    
  	  C1.add(val.toString());
    	
    }
    Collections.sort(C1);
    
    for(int i=0;i<C1.size();i++) {
    	    for(int j=i+1;j<C1.size();j++) {
    	       	String C2="";
    	    	    C2 = C1.get(i) + "," + C1.get(j);
    	    	    context.write(new Text(C2), null);
    	    	    
    	    }
    }
   
	  
     
  }
  else {
	 // System.out.println("I'm Here");
	  List<String> C1 = new ArrayList<String>();
	  for(Text val : values) {
			  C1.add(val.toString());
	 }
	  Collections.sort(C1);
	 // System.out.println("YOO"+C1);
	  if(C1.size()!=1) {
		  for(int i=0;i<C1.size();i++) {
	    	    for(int j=i+1;j<C1.size();j++) {
	    	       	String C2="";
	    	    	    C2 = key.toString() +","+ C1.get(i) + "," + C1.get(j);
	    	    	    context.write(new Text(C2), null);
	    	    	    
	    	    }
	    }
	  }
	  
		  
  }
}
}
public static class FMapper
extends Mapper<Object, Text, Text, IntWritable>{
private String T = new String();
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
 T = itr.nextToken().toString();
 List<Integer> T_value = new ArrayList<Integer>();
  String[] T_splites = T.split(" ");
  for(String s:T_splites) {
	  T_value.add(Integer.parseInt(s));
	  //System.out.println(s);
  }
  
 int max_utility_T = T_value.get(T_value.size()-1);
 Configuration conf = context.getConfiguration();
 String temp = conf.get("C_Path"); //Location of file in HDFS
 //System.out.println("BBBB:"+temp);
 Path pt = new Path(temp+"/part-r-00000");
 FileSystem fs = FileSystem.get(new Configuration());
 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
 String line;
 line=br.readLine();
 while (line != null){
     System.out.println("HA:"+line+" Length:"+line.length());
     String[] T_line = line.split(",");
     int sum =1;
     for(String s_line : T_line) {
    	 System.out.println("Debug:"+s_line);
    	   sum = sum *T_value.get(Integer.parseInt(s_line)-1);
     }
     
     if(sum!=0) {
    	  System.out.println("Line is:"+line+" utility value is:"+max_utility_T+" The T is:"+T_value);
    	   context.write(new Text(line), new IntWritable(max_utility_T));
     }
     line=br.readLine();
    
    		 
 }
 
  
}
}
}
public static class FSumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
int sum = 0;
int TH=45;
for (IntWritable val : values) {
 sum += val.get();
}

  result.set(sum);
  if(sum>=TH) {
     context.write(key, result);
  }
}
}
static List<Integer> profit_table = new ArrayList<Integer>();
public static void main(String[] args) throws Exception {

  Configuration conf = new Configuration();
  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  if (otherArgs.length != 2) {
  //  System.err.println("Usage: <in><out>");
   
  //  System.exit(2);
  }
  Job job = Job.getInstance(conf, "HUIT Mining");
  job.setJarByClass(HUIM.class);
  
 //Path profit = new Path(otherArgs[0]);
 //Path inputPath = new Path(otherArgs[1]);
 //Path outputPath = new Path(otherArgs[2]);
  
  Path profit = new Path("/ethonwu/profit.txt");
  Path inputPath = new Path("/ethonwu/HUIM.txt");
  Path outputPath = new Path("/ethonwu/HUIM_temp/");
  int stage = 1;
  outputPath.getFileSystem(conf).delete(outputPath, true);
  
  FileSystem fs = FileSystem.get(conf);
  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(profit)));
  String line;
  line = br.readLine();
  while(line!=null)
  {
	  //System.out.println(line);
	  String[] splite_line = line.split(" ");
	  profit_table.add(Integer.parseInt(splite_line[1]));
	  line = br.readLine();
  }
  //System.out.println(profit_table);
  
 
  
  job.setMapperClass(TokenizerMapper.class);
  job.setReducerClass(IntSumReducer.class);

  

  FileInputFormat.addInputPath(job, inputPath);
  //Test
 
  FileOutputFormat.setOutputPath(job,outputPath);
  job.setNumReduceTasks(1);
  job.setOutputKeyClass(Text.class);
 // job.setOutputValueClass(IntWritable.class);
  job.setOutputValueClass(Text.class);

// System.exit(job.waitForCompletion(true) ? 0 : 1);
  while(!job.waitForCompletion(true)) {}
  
  Path prune_output = new Path("hdfs:/ethonwu/HUIM_output/"+Integer.toString(stage));
  prune_output.getFileSystem(conf).delete(prune_output, true);
  Job job2 = new Job(conf, "Prune part");
  job2.setJarByClass(HUIM.class);
  
  job2.setMapperClass(PruneMapper.class);
  job2.setReducerClass(IntSumCombiner.class);
  
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(IntWritable.class);
  
  
  FileInputFormat.addInputPath(job2, outputPath);
  FileOutputFormat.setOutputPath(job2,prune_output);
  
  job2.waitForCompletion(true);
  
  //while(!job2.waitForCompletion(true)) {}
  Counters count = job2.getCounters();
  long info = count.getGroup("org.apache.hadoop.mapreduce.TaskCounter").findCounter("REDUCE_OUTPUT_RECORDS").getValue();
  int nums = (int) (long) info;
 // long info = job2.getCounters().findCounter("Map-Reduce Framework","Reduce output records").getValue();
   System.out.println("Now run Here:"+nums);
   if(nums==0) {
	   System.out.println("Find Finish!!");
	   System.exit(0);
   }
  
  // Job 3 Link F1 part  
  // Path generate_C2 = new Path("hdfs:/ethonwu/generate_temp/");
  // stage++;
  // Path C_output = new Path("hdfs:/ethonwu/Generate/C"+Integer.toString(stage));
  // generate_C2.getFileSystem(conf).delete(generate_C2, true);
  // C_output.getFileSystem(conf).delete(C_output, true);
   int hey=0;
  
  while(true) {
	 
	  Path FUIT = new Path("hdfs:/ethonwu/HUIM_output/"+Integer.toString(stage));
	  stage++;
	Path  C_output = new Path("hdfs:/ethonwu/Generate/C"+Integer.toString(stage));
	
   Job job3 = new Job(conf, "Generate Candidate 2");
   job3.setJarByClass(HUIM.class);
   job3.setMapperClass(LinkMapper.class);
   job3.setReducerClass(LinkReducer.class);
   job3.setOutputKeyClass(Text.class);
   job3.setOutputValueClass(Text.class);
  
   FileInputFormat.addInputPath(job3, FUIT);
  
   FileOutputFormat.setOutputPath(job3,C_output);
   
   job3.waitForCompletion(true);
   System.out.println("Try:"+stage);
   Counters count2 = job3.getCounters();
   long info2 = count2.getGroup("org.apache.hadoop.mapreduce.TaskCounter").findCounter("REDUCE_OUTPUT_RECORDS").getValue();
   int nums2 = (int) (long) info2;
  // long info = job2.getCounters().findCounter("Map-Reduce Framework","Reduce output records").getValue();
    System.out.println(nums2);
    if(nums2==0) {
 	   System.out.println("Find Finish!!");
 	   break;
 	//   System.exit(0);
    }
   
   
    conf.set("C_Path", C_output.toString());
  // Job4 Check is FUIT or not  
     FUIT = new Path("hdfs:/ethonwu/HUIM_output/"+Integer.toString(stage));
    
    Job job4 = new Job(conf,"Get FUIT");
    job4.setJarByClass(HUIM.class);
    
    job4.setMapperClass(FMapper.class);
    job4.setReducerClass(FSumCombiner.class);
   
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(IntWritable.class);
   
    FileInputFormat.addInputPath(job4,outputPath );
    FileOutputFormat.setOutputPath(job4,FUIT);
    
    job4.waitForCompletion(true);
    
    
 //   break;
   
  }
 System.exit(0);
}


}
