#!/bin/bash

#SBATCH --nodes=1
#SBATCH --output=linsort2GB.log

START_TIME=$SECONDS

LC_ALL=C sort --parallel=4 -o /tmp/datasorted.out /input/data-2GB.in

ELAPSED_TIME=$(($SECONDS-$START_TIME))

echo  "Total time taken for sorting 2 GB file in 4 threads using lin sort is"

echo "$ELAPSED_TIME"

valsort /tmp/datasorted.out
