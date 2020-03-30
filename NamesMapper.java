package bigdataAssignment1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.io.TextPair;

public class NamesMapper extends Mapper<LongWritable, Text, Text, TextPair> {

	public NamesMapper() {
		System.out.println("NamesMapper ...");
	}

	private NamesParser parser = new NamesParser();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			parser.parse(value.toString());

			context.write(new Text(parser.getNconst()), new TextPair(parser.getPrimaryName(), "name"));
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

}
