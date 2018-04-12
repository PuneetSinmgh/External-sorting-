import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class MySort {

		private BufferedReader br;
	public static void main(String[] args) throws IOException,InterruptedException {
	
		
		String inputdir = "/input/";
		String tempunsorteddir = "/tmp/unsorted/";
		String tempsorteddir = "/tmp/sorted/"; 
		SortChunks sorting = new SortChunks();
		MySort  exec = new MySort();
		
		System.out.println(inputdir+args[0]);
		exec.createchunks(inputdir+args[0]);
	
		File chunkdir = new File(tempunsorteddir);
		File[] unsortedchunks =  chunkdir.listFiles();

		CountDownLatch doneSignal = new CountDownLatch(unsortedchunks.length);
		ExecutorService executor = Executors.newFixedThreadPool(12);


		int i;
		for( i =0 ; i< unsortedchunks.length; i++) {
			
			File chunk = unsortedchunks[i];

			executor.execute(new Worker(doneSignal,chunk, sorting));		

		}
		executor.shutdown();
	
	
		 doneSignal.await();
	
		for(File file: unsortedchunks){
			System.out.println("\ndelete sorted :"+ file.getPath());
			file.delete(); 

		}	
		exec.mergechunks();
		
		
	}

	
	public void createchunks(String Input) throws IOException {
		
		ArrayList< String> lines = new ArrayList<>();
		int file=0,currentblock=0;
		long blocksize=1000 ;
	
	  
		  File inputfile = new File(Input);
		  System.out.println("\ninput file size:"+inputfile.length());
	 	 if(inputfile.length()== 2000000000) {

			blocksize = 50000000;
	       	}
		else {
			blocksize = 40000000;
		
		}
		System.out.println("creating chunks");
		try {
			System.out.println(Input);
			br = new BufferedReader(new FileReader(Input));
		
	
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		int linecount = 1;

		String line = " ";
		while(line!=null) {
		   file++;	
		   linecount=1;
			while((linecount <=blocksize/100) && (line=br.readLine())!=null) {
				lines.add(line);
				linecount++;
			}
			
			if(line==null) {
                		break;
                	}
		
		  createUnsortedTempFiles(lines,file);
		  lines.clear();
		}
		
		
	}
	
	public void createUnsortedTempFiles(ArrayList<String> lines,int fileno) {
		
		FileWriter writer = null;
		
		File dir = new File("/tmp/unsorted/");
		
		if(!dir.exists()){ //In case directory does not exists it will create a new directory
			dir.mkdir();
		}
		System.out.println("\nwriting in file:"+fileno);
		try {

			writer = new FileWriter("/tmp/unsorted/"+fileno+".txt");
			for (String str: lines) {
	        	 writer.write(str+"\r\n");
		    }
	        writer.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}					
		
	}

	public void mergechunks() throws IOException {
		
		List<File> files = new ArrayList<File>();
		File sorted = new File ("/tmp/sorted/");
		File[] sortedFiles = sorted.listFiles();
		
		File outputfile = new File("/tmp/sorted.out"); 
		
		for(File file: sortedFiles){
			if(file.isFile()){ 
				files.add(file);
			}
		}
		Comparator<String> cmparator = new Comparator<String>() {
			public int compare(String s1, String s2){
                return s1.compareTo(s2);
            }
		};
	
        PriorityQueue<Minheapnode> pqueue = new PriorityQueue<Minheapnode>(11, 
            new Comparator<Minheapnode>() {
              public int compare(Minheapnode i, Minheapnode j) {
		return cmparator.compare(i.getLine().substring(0, 10), j.getLine().substring(0, 10));
              }
            }
        );
        
        for (File f : files) {
            Minheapnode bfb = new Minheapnode(f);
            pqueue.add(bfb);    
        }
        files.clear();
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));

        try {
            while(pqueue.size()>0) {
                Minheapnode heapnode = pqueue.remove();
                String r = heapnode.getvalue();
                bw.write(r+"\r\n");
	   
                if(heapnode.empty()) {

			heapnode.line = null;
                	heapnode.fr.close();
			heapnode.originalfile.delete();
               }
		else {
                	pqueue.add(heapnode);
		
                }

            }
        } catch (Exception iOException) {
			
		}
        bw.close();
	}
	
	class Minheapnode{
  
	    private String line;
	    private boolean empty;
	    public BufferedReader fr;
	    public File originalfile;
   
	    public Minheapnode(File f) throws IOException {
	      
		originalfile = f;
	        fr = new BufferedReader(new FileReader(f));
	        reload();
	    }
	     
	    public boolean empty() {
	        return empty;
	    }
	     
	    private void reload() throws IOException {
	      
	          try {
	          if((line = fr.readLine()) == null){
	            empty = true;
	            line = null;
	            fr.close();
	            System.out.println("Eof");
	          }
	          else{
	            empty = false;
	          }
	    	} catch(EOFException oef) {
	              empty = true;
	              line = null;
	            }
	    }
	    
	    public String getLine() {
	        if(empty()) return null;
	        return line.toString();
	    }
	    public String getvalue() throws IOException {
	      String answer = getLine();
	       line = null;
		 reload();
	      return answer;
	    }
	}

	static class Worker implements Runnable {
		   private final CountDownLatch doneSignal;
		   private File s;
		   private SortChunks sort;
		   Worker(CountDownLatch doneSignal, File str,SortChunks sort) {
		      this.doneSignal = doneSignal;
		      this.s = str;
		      this.sort = sort;
		   }
		   public void run()  {
		      try {
		        sort.readAndSortChunk(s);
		        doneSignal.countDown();
		      } catch (IOException ex) {} // return;
		   }
		}	
}

