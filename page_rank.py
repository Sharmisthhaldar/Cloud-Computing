# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 17:20:55 2019

@author: sharm
"""
import sys
import re
from operator import add

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	
    # create Spark context 
    sc = SparkContext("local","PySpark Page Ranking")
    
    # read data from text file and calculate number of pages in the corpus
    pages = sc.textFile(sys.argv[1]).filter(lambda line: line.strip())
    pageCount = pages.count()
    print(pageCount)
    
    def add_outlinks(line):
        str = ''
        # Extract the contents within text tag 
        text = re.search('<text.*?>(.*)</text>', line)
        if text:
            text = text.group(1)
            # Find directed links using the patter [[*]]
            outlinks = re.findall('\[\[(.*?)\]\]', text)
            
            # Return a output string with all links stored in a faction of @@link1@@@@link2@@@@link3@@
            for link in outlinks:
                str = str+'@@'+link+'@@'

        return str
   
    def calculate_page_rank(line):
        if len(line[1]) > 0 and line[1][1]:
            outlinks_str = line[1][1]
            rank = line[1][0]
            # Find all the links by extracting the text uisng pattern match @@*@@
            outlinks = re.findall('\@\@(.*?)\@\@', outlinks_str)            
            length = len(outlinks)
            
            # Calculate page rank for each links
            for link in outlinks:
               yield (link, float(float(rank)*1.0/length))
            
            # Also pass the main link for join operation of RDD's
            if length != 0:  
              yield(line[0],0)  
              
    
    # Creating link RDD
    link_rdd = pages.map(lambda line: (re.search('<title>(.+?)</title>', line).group(1), add_outlinks(line))).cache()
    
    # Creating rank RDD
    # Assign initial page rank of 1/pageCounts 
    initial_rank_rdd = pages.map(lambda line: (re.search('<title>(.+?)</title>', line).group(1), 1*1.0/pageCount))
    
    # Joining link and rank rdd
    page_rank_rdd = initial_rank_rdd.join(link_rdd).cache()
    
    # Calculating the new rank of the page in the corpus (10 iterations)   
    for i in range(10):
        # Calcute the page rank for all the links and add them 
        pageRank_rdd = page_rank_rdd.flatMap(lambda line: calculate_page_rank(line)).reduceByKey(add)
        # Add a damping factor of 0.85 to the calculated page ranks
        ranks_rdd = pageRank_rdd.mapValues(lambda rank: rank * 0.85 + 0.15)
            
        #Joining link and rank rdd        
        page_rank_rdd = ranks_rdd.join(link_rdd).cache()        
        
    # Sorting
    output_rdd = page_rank_rdd.map(lambda rank: (rank[0] , rank[1][0])).cache()
    sorted_page_rank = output_rdd.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0])).take(100)    
    
    # Save the RDD
    sc.parallelize(sorted_page_rank).saveAsTextFile("PageRank_Sorted")
    
    sc.stop()

    
