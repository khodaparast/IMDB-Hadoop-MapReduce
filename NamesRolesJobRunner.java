package bigdataAssignment1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdataBook.JoinRecordMapper;
import bigdataBook.JoinRecordWithStationName;
import bigdataBook.JoinReducer;
import bigdataBook.JoinStationMapper;
import bigdataBook.JoinRecordWithStationName.KeyPartitioner;
import common.JobBuilder;
import common.io.TextPair;

public class NamesRolesJobRunner extends Configured implements Tool {
	
	
	public NamesRolesJobRunner() {
		System.out.println("JobRunner ..." );
	}
  
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<roles input> <names input> <output>");
      return -1;
    }
    
	Job job = new Job(getConf(), "Join names primaryName with roles records");
    job.setJarByClass(getClass());
    
    Path rolesInputPath = new Path(args[0]);
    Path namesInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    MultipleInputs.addInputPath(job, rolesInputPath,
        TextInputFormat.class, RolesMapper.class);
    
    MultipleInputs.addInputPath(job, namesInputPath,
        TextInputFormat.class, NamesMapper.class);
    
      
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.setMapOutputKeyClass(Text.class);
    
    job.setReducerClass(RoleNameMovieReducer.class);

    job.setOutputKeyClass(Text.class);

    job.setMapOutputValueClass(TextPair.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new NamesRolesJobRunner(), args);
    System.exit(exitCode);
  }
}



