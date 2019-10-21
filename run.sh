#!/bin/bash
#
##################################################################
# Map Reduce Analysis - Jason D Stock - stock - October 12, 2019 #
##################################################################
#
# Run script for the MapReduce application.
#

JAR_FILE="mapreduce-analysis-flights.jar"

# USAGE FUNCTION

function usage {
cat << EOF
    
    Usage: run.sh -[ 1 | 2 ] -c -s

    -1 : Basic Questions Q1, Q2
    -2 : Hub Questions Q3, Q6
    
    -c : Compile
    -s : Shared HDFS
    
EOF
    exit 1
}

# JOB RUNNER

function hadoop_runner {
$HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${CLASS_JOB} ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs555.hadoop.${CLASS_JOB}.Driver \
$FIRST_INPUT $SECOND_INPUT ${OUT_DIR}/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -ls ${OUT_DIR}/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -head ${OUT_DIR}/${CLASS_JOB}/part-r-00000
}

# APPLICATION CONFIGS

if [[ $# -lt 1 ]]; then
    usage;
fi

if [[ $* = *-c* ]]; then
    find ~/.gradle -type f -name "*.lock" | while read f; do rm $f; done
    gradle build
    
    LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`

    echo Project has "$LINES" lines
fi

if [[ $* = *-s* ]]; then
    export HADOOP_CONF_DIR=${HOME}/hadoop/client-config
    HDFS_DATA="data"
    OUT_DIR="/home/out"
else
    HDFS_DATA="local"
    OUT_DIR="/out"
fi

FIRST_INPUT="/"${HDFS_DATA}"/main/*.csv"
SECOND_INPUT="/${HDFS_DATA}/supplementary/"

case "$1" in
    
-1) CLASS_JOB="time"
    SECOND_INPUT=""
    hadoop_runner
    ;;
    
-2) CLASS_JOB="hubs"
    SECOND_INPUT+="airports.csv"
    hadoop_runner
    ;;
    
*) usage;
    ;;
    
esac
