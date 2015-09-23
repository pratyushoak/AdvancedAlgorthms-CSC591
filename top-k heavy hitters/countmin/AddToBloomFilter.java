package storm.starter.trident.project.countmin;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import storm.starter.trident.project.countmin.filters.BloomFilter;
import java.io.*;

/**
 *@author: Pratyush Oak
 */

public class AddToBloomFilter extends BaseFilter
{

BloomFilter bloomFilter;
String stopWordsLocation; //will hold path of the file containing stop-words
int word_count=0; //will hold number of stop words
int bits=8; //bits per bloomFilter object
int hash=5; //number of hash functions

public AddToBloomFilter()
{	
	//location of the stop words file
	stopWordsLocation="data/stop-words.txt";
	// count number of words in the file by getting number of lines ; source:stackoverflow.com
	BufferedReader reader;
	try{
	reader = new BufferedReader(new FileReader(stopWordsLocation));
	while (reader.readLine() != null) word_count++;
	reader.close();
	}
	catch(Exception e){
 		System.out.println("Missing File!");
 	}
	//System.out.println("words:"+word_count);

	//create a new bloomfilter to hold word_count words 
	bloomFilter=new BloomFilter(word_count,bits,hash);

	String s;
	BufferedReader br=null;
	try{

		//read each word from the file and add it to the bloomfilter
 		br = new BufferedReader(new FileReader(stopWordsLocation));
		while ((s = br.readLine()) != null) {
			bloomFilter.add(s);
		}
	}
	catch(Exception e){
 		System.out.println("Missing File!");
 	}
}
	@Override
public boolean isKeep(TridentTuple tuple) {
//System.out.println(bloomfilter.contains(tuple.getString(0)));
	//check each incoming stream word with the bloom filter words 
	//and return false if it exists in bloomfilter
return !(bloomFilter.contains(tuple.getString(0)));
}

}