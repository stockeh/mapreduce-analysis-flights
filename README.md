# Distributed Data Analytics

**Analyzing on-time performance of commercial flights in the United States using MapReduce**

This project focuses on using Apache Hadoop ( version 3.1.2 ) to develop MapReduce programs to analyze a [dataset](http://www.transtats.bts.gov/Fields.asp?Table_ID=236) released by the United States Bureau of Transportation Statistics. The data contains information specific to the performance records for commercial flights operated by major carriers in the United States from October, 1987 to April, 2008. In total there is approximately 9 GB of data, consisting of roughly 120 million records, dispersed among 20 machines in a HDFS cluster.  

Structuring the dataset within a cluster as such allows for real time analytics on various questions, including, but not limited to, what is the best/worst time-of-day/day-of-week/time-of-year to be flying, does the East or West coast have more delays, which airports contribute the most towards late aircraft delays for connecting flights?  

A thorough review of this dataset and the associated questions can be found [[ here ](https://github.com/stockeh/mapreduce-analysis-flights/blob/master/written-analysis.pdf)].
