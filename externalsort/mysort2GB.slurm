#!/bin/bash

#SBATCH --nodes=1
#SBATCH --output=mysort2GB.log

javac *.java
chmod 775 *.*
echo 'start sorting'

time java -cp . -Xms2G -Xmx7G MySort data-2GB.in
echo 'end sorting'

valsort /tmp/sorted.out
