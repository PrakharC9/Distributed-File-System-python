#!/usr/bin/env bash
cnt=${1:-2} #command line param $1 [default value=2]
for i in $( seq -f "%03g" 1 $cnt ) #pad with 0s 
    do
        fuser -k -n tcp 11$i #free port
    done
for i in $( seq -f "%03g" 1 $cnt ) 
    do        
        python3 server.py 11$i & #run server
    done