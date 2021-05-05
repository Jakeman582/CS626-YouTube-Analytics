# CS626-YouTube-Analytics
This repository contains a collection of scripts and MapReduce jobs that perform an analysis on YouTube videos in an attempt to train an OpenAI GPT-2 model to generate new ideas for future YouTube videos.

This analysis can be broken up into 3 stages:

1.) Data Scraping

Python scripts are used to contact the YouTube API and collect video metadaata for a given input file containing YouTube channels.

2.) Video Title Analysis

A set of MapReduce jobs are used to calculate each channel's average number of views, average number of comments, and average like:dislike ratio. We then filter each channel's videos to produce a list of titles that have a higher-than-average view count, comment count, and like:dislike ratio.

3.) Channel Clustering

Based on the words used in the titles of YouTube videos, a set of MapReduce jobs attempt to cluster YouTube channels into related groups.

***Overarching goals***

We want to be able to assist YouTube content creators with creating new and successful videos by examing the past success of their videos and predicting new video titles that will be positively engaging.

In addition, we want to be able to help provide YouTube viewers new content to watch based on their past viewing experiences.

**Data Collection**

-> Source Files: scrape_data.py, YouTubeAPI.py

-> Input: input_channels.txt

To get the view, like, dislike and comment count for the supplied YouTube channels, along with their associated video titles. Creates new files containing these statistics in separate files for each YouTuber.

For the **scrape_data.py call()** function, **file_name** refers to the widely known Youtube name and **user_name** refers to the Youtube username (found in the URL of the channel's page on YouTube.)

    Example usage
      linux> python3 scrape_data.py <input_channels.txt>

**Video Title Analysis**
There are 3 MapReduce jobs defined in 3 separate packages for performing video title analysis that must be performed in sequence.

1.) The Converter package
    This package contains the **SmallFileToSequenceFileConverter** job in order to aggregate all of the files produced in the data collection phase
    
    Example usage:
      linux> hadoop jar Project.jar Converter.SmallFileToSequenceFileConverter <input_directory> <output_directory>

2.) The Analyser package
    This package takes the SequenceFile generated as loutput from step 1 to calculate per-channel averages and global averages.
    Currently, the output directory must be specified as "output/Project/ChannelFile" for the Filter MapReduce job as it has been hardcoded.


    Example usage:
      linux> hadoop jar Project.jar Analyser.YouTubeAnalyser <output_file_from_step_1> output/Project/ChannelFile

3.) The Filter package
    This package contains a MapReduce job to filter out videos for each YouTuber that do not have higher-than-average metrics, and produces files for every YouTuber containing only thoise titles tnhat should be used to train an NLP engine to generate new sentences.
    
    Example usage:
      linux> hadoop jar Project.jar Filter.VideoFilter <output_file_from_step_1> <new_directory>

**Channel Clustering**
There are 3 MapReduce jobs defined in 3 separate packages for performing channel clustering, again which must be performed in sequence (after completing step 1 in the video title analysis section.)

4.) The Lister package
    This package searchesa through every single video title collected from the API and lists out the words that have been used, and stores them in a file so they can be refernced later. As of now, the output_directory must be specified as "output/Project/GlobalWordCount/" since the job in step 5 reads from this location directly.
    
    Example usage:
      linux hadoop jar Project.jar Lister.TitleWordCounter <output_file_from_step_1> output/Project/GlobalWordCount/

5.) The Counter package
    This package goes through every YouTuber's list of videos and does a word count. The word counts are stored in a vector whose components represent the ordered list of words found in step 1 of the clustering procedures. All word vectors are stored in one output file for the final procedure.
    
    Example usage:
      linux> hadoop jar Project.jar Counter.ChannelWordCounter <output_file_from_step_1> <new_output_directory>

6.) The Cluster package
    This package contains the procedures to perform k-means clustering on the output word vector file from step 2 of the clustering procedures. Multiple different numbers of clusters are tried, and cluster re-assignment continues until cluster convergence is acheived. The results for each number of clusters, and iteration, are stored in separate directories within the output_directory specified on the command line.
    
    Example usage:
      linux> hadoop jar Project.jar Cluster.ChannelCluster <output_from_step_5>
