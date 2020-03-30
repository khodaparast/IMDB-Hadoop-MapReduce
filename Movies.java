package bigdataAssignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Multiset.Entry;

public class Movies {
	private String filePath;
	
public HashMap<String, String> getWest2010Movies() throws IOException{
	
	HashMap<String, String> result=new HashMap<String, String> ();
	
	BufferedReader br=new BufferedReader(new FileReader(new File("/input/movies.tsv")));
    String line;
    String[] elements;

    while((line=br.readLine()) != null) {
    	elements=line.split("\t");
		try {
			if(Integer.parseInt(elements[2]) > 2010 && elements[3].contains("Western")) {
				result.put(elements[0], elements[1] + "\t" + elements[2]);
			}
		}catch(Exception e) {
			continue;
		}
    }
	
	return result;
		
	}


}
