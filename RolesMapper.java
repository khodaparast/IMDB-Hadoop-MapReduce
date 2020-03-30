package bigdataAssignment1;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import common.io.TextPair;

public class RolesMapper extends Mapper<LongWritable, Text, Text, TextPair> {

	Movies mo = new Movies();
	HashMap<String, String> western2010movies;
	RolesParser parser = new RolesParser();

	public RolesMapper() throws IOException {
		System.out.println("RolesMapper ...");

		western2010movies = mo.getWest2010Movies();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int isDirector = parser.parse(value.toString());

		if (isDirector == 1) {

			context.write(new Text(parser.getNconst()), new TextPair(parser.getTconst(), "role"));
		}
	}

}
