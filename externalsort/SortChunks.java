
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Comparator;
import java.util.Scanner;


public class SortChunks {


	BufferedReader br;
	
	public SortChunks() { 
		
		
	}
	
	
	
	public void readAndSortChunk(File unsortedchunk) throws IOException{
			
		ArrayList<String> allkeys = new ArrayList<>();
		ArrayList<String> sortedkeys = new ArrayList<>();
		Hashtable<String,String> alllines = new Hashtable<>();
		ArrayList<String> sortedLines = new ArrayList<>();
		
		
		try {
			br = new BufferedReader(new FileReader(unsortedchunk.getPath())); // filereader needs filepath 
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line = null;
		Scanner scanner = new Scanner(unsortedchunk);
		while(scanner.hasNextLine()) {
			
			line=scanner.nextLine();
			String key = line.substring(0,10);
			String value = line.substring(10,line.length());
			
			allkeys.add(key);
			alllines.put(key, value);
			
		}
		

		sortedkeys=mergesort(allkeys);
		
		for (String s : sortedkeys) {
			sortedLines.add(s+alllines.get(s));
		}
		
		allkeys.clear();
		sortedkeys.clear();
		alllines.clear();
		String s = "/tmp/sorted/"+unsortedchunk.getPath().substring(14, unsortedchunk.getPath().length());
		writeLines(sortedLines,s);

	}

	
	public ArrayList<String> mergesort(ArrayList<String> keys) {
		
		ArrayList<String> left = new ArrayList<String>();
	    ArrayList<String> right = new ArrayList<String>();
	    int center;
	 
	    if (keys.size() == 1) {    
	        return keys;
	    } else {
	        center = keys.size()/2;
	        
	        for (int i=0; i<center; i++) {
	                left.add(keys.get(i));
	        }
	        
	        for (int i=center; i<keys.size(); i++) {
	                right.add(keys.get(i));
	        }
	        
	        left  = mergesort(left);
	        right = mergesort(right);
	 
	        // Merge the results back together.
	      
	        merge(left, right, keys);
	    }
	    return keys;
		
	}
	
	public void merge(ArrayList<String> left, ArrayList<String> right, ArrayList<String> keys) {
		
		int l = 0;
	    int r = 0;
	   
	    int keysIndex = 0;
	    
	    while (l < left.size() && r < right.size()) {
	    	
	        if ( (left.get(l).compareTo(right.get(r))) < 0) {
	        	keys.set(keysIndex, left.get(l));
	            l++;
	        } else {
	        	keys.set(keysIndex, right.get(r));
	            r++;
	        }
	        keysIndex++;
	    }

	    while(l<left.size()) {
	    	keys.set(keysIndex,left.get(l));
	    	l++;
	    	keysIndex++;
	    }
	    while(r<right.size()) {
	    	keys.set(keysIndex,right.get(r));
	    	r++;
	    	keysIndex++;
	    }	

	} 
	
	public void writeLines(ArrayList<String> lines, String s) throws IOException {
		
		File dir = new File("/tmp/sorted/");
		if(!dir.exists()){ //In case directory does not exists it will create a new directory
			dir.mkdir();
		}  
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(s));
		for(String str: lines) {
			bw.write(str+"\r\n");
		}
		System.out.println("\nfile sorted written:"+s);
		lines.clear();
		bw.close();
	}
	
}

