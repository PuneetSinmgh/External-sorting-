#!/bin/bash

#SBATCH --nodes=1
#SBATCH --output=mysort20GB.log

javac *.java
chmod 775 *.*
echo 'start sorting'

time java -cp . -Xms2G -Xmx7G MySort data-20GB.in
echo 'end sorting'

valsort /tmp/sorted.out

