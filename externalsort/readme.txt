1. Makefile compiles all the java files present in the current directory and creates their executable class files.
2. There are 4 slurm files which can be used to trigger sorting experiments. 
3. Two files are for linSort comamnd to perform sorting named linsort2GB.slurm and linsort20GB.slurm 
4. The output generated are stored in  linsort2GB.log and linsort20GB.log 
5. mysort2GB.slurm and mysort20GB.slurm are used to trigger Shared memory implementation.
6. Output log are stored into mysort2GB.log and mysort20GB.log
7. MySort.java is the main class java file , this file creates chunks and call SortChunks class tosort unsorted chunks.
8. SortChunks.java contains methods to sort the unsorted data.
9. There are several other annonymous classes named Mysort$1.class , MySort$2.class and SortChunks$1.class these are the annonnymous inner classes used in MySort and SortChunks class.

	INPUT : /tmp/data-2GB.in and /tmp/data-20GB.in
	OUTPUT: mysort2GB.log and mysort20GB.log
	
	Makefile can be triggered just by using Makefile file command.
	Slurm scripts can be executed using sbach command.


