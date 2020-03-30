package bigdataAssignment1;

import org.apache.hadoop.io.Text;

public class RolesParser {

	
	private String tconst;
	private String nconst;
	private String category;
	
	
	
	 public int parse(String record) {
		 int directorFlag = 0;
		 
		 String[] elements=record.split("\t");

		 tconst=elements[0];
		 nconst=elements[2];
		 category=elements[3];	
		 if(category.equalsIgnoreCase("director"))directorFlag=1;
		 return directorFlag;
		 		 
	 }
	 
	 public void parse(Text record) {
		    parse(record.toString());
		 }
	 
	 public String getTconst() {
		 return this.tconst;
		 
	 }
	
	 public String getNconst() {
		 return this.nconst;
	 }
		 
	 public String getCategory() {
		 return this.category;
	 }
	
}
