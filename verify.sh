#!/bin/bash

for i in {0..8}; do 
    echo `grep -o , data/main/200$i.csv | wc` 
    echo `wc -l data/main/200$i.csv` 
done

for i in {7..9}; do 
    echo `grep -o , data/main/198$i.csv | wc` 
    echo `wc -l data/main/198$i.csv` 
done

for i in {0..9}; do 
    echo `grep -o , data/main/199$i.csv | wc` 
    echo `wc -l data/main/199$i.csv` 
done
