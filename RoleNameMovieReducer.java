package bigdataAssignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import common.io.TextPair;

public class RoleNameMovieReducer extends Reducer<Text, TextPair, Text, Text> {
	Movies movies = new Movies();

	HashMap<String, String> moviesMap = new HashMap<>();

	public RoleNameMovieReducer() {
		System.out.println("RoleNameMovieReducer ...");
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		moviesMap = movies.getWest2010Movies();
	}

	@Override
	protected void reduce(Text key, Iterable<TextPair> values, Context context)
			throws IOException, InterruptedException {

		Iterator<TextPair> iter = values.iterator();

		String directorName = null;
		List<String> movieIds = new ArrayList<String>();

		while (iter.hasNext()) {
			TextPair line = iter.next();
			String type = line.getSecond().toString();


			if (type.equals("name")) {
				directorName = line.getFirst().toString();
			} else {
				movieIds.add(line.getFirst().toString());
			}
		}

		if (directorName != null) {
			for (String tconst : movieIds) {
				if (moviesMap.get(tconst) != null) {
					Text outValue = new Text(directorName + "\t" + moviesMap.get(tconst));
					context.write(key, outValue);
				}
			}
		}
	}
}