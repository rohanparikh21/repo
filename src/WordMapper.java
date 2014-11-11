import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	// Mapper class for our inverted index

	  
	// All the unwanted words whose inverted index is not needed
	  
	  String[] stopwords = {"a", "able", "about",
				"across", "after", "all", "almost", "also", "am", "among", "an",
				"and", "any", "are", "as", "at", "be", "because", "been", "but",
				"by", "can", "cannot", "could", "dear", "did", "do", "does",
				"either", "else", "ever", "every", "for", "from", "get", "got",
				"had", "has", "have", "he", "her", "hers", "him", "his", "how",
				"however", "i", "if", "in", "into", "is", "it", "its", "just",
				"least", "let", "like", "likely", "may", "me", "might", "most",
				"must", "my", "neither", "no", "nor", "not", "of", "off", "often",
				"on", "only", "or", "other", "our", "own", "rather", "said", "say",
				"says", "she", "should", "since", "so", "some", "than", "that",
				"the", "their", "them", "then", "there", "these", "they", "this",
				"tis", "to", "too", "twas", "us", "wants", "was", "we", "were",
				"what", "when", "where", "which", "while", "who", "whom", "why",
				"will", "with", "would", "yet", "you", "your"};
	  
	StringTokenizer st = new StringTokenizer(value.toString());
	int count = 0;
	Text url = new Text();
	Text word = new Text();
	// If Emit is set to false then mapper will not write to context
	boolean emit = false;
	while(st.hasMoreTokens()){
		if(count == 0){
			url.set(st.nextToken());
		}
		else{
			emit = true;
			String temp = st.nextToken().toLowerCase();
			String englishCheck = null;
			System.out.println(temp);
			if(temp.matches("[a-zA-Z]+")){
				System.out.print(temp);
				emit = false;
				System.out.println(emit);
			}
			// Checking if english word or not
			try{
				BufferedReader in = new BufferedReader(new FileReader(
			              "/usr/share/dict/words"));
				//while((englishCheck = in.readLine()) != null){
				//	if(englishCheck.indexOf(temp) != -1){
				//		emit = true;
				//	}
				//}
				in.close();
			}catch(IOException e){
				e.printStackTrace();
			}
			for(int i=0; i<stopwords.length; i++){
				if(temp == stopwords[i]){
					emit = false;
				}
			}
			// Stemming the word by using porter stemmer
			Stemmer s = new Stemmer();
			char[]  chars = temp.toCharArray();
			for(int i=0; i<chars.length; i++){
				s.add(chars[i]);
			}
			s.stem();
			word.set(s.toString());
		}
		count++;
		if(emit){
			context.write(word, url);
		}
	}
	  
	
  }
}
