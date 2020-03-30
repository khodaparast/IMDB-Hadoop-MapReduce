package bigdataAssignment1;

import org.apache.hadoop.io.Text;

public class NamesParser {
	
	private String nconst;
	private String primaryName;
	
	 public void parse(Text record) {
		   parse(record.toString());
		  }
	
	 public void parse(String record) {
		 
		 String[] elements=record.split("\t");

		 nconst=elements[0];
		 primaryName=elements[1];
		 		 		 
	 }
	 
	 public String getNconst() {
		 return this.nconst;
		 
	 }
	 public String getPrimaryName() {
		 return this.primaryName;
	 }
	
}
