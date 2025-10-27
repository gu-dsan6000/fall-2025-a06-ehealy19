# Analysis Report

## Description of Approach to Problems

For problem 1, my approach was to build off of the code from the previous lab where we setup a spark cluster. I then created a function to create a spark session and a function to save csv files instead of folders as Spark does. Finally, I made a function that read in the log files, got the log level categories using a regex function, and counted the number of logs for each category. I then got 10 samples utilizing a select and orderby function. Finally, I computed the summary statistics and saved all these outputs to CSV using my function.

For problem 2, I took the skeleton of problem 1, resuing the main functions in the script. After reading in the log files, I got the time patterns utilizing a regex function as well as grabbed the timestamps. Then I got the application times by using a filter, groupby, and agg function. I then computed the summary statistics, similar to problem 1. For the bar chart and density plot I utilized matplotlib to create plot of these plots. Finally, I stopped the Spark session and exported the outputs. 

## Key Findings and Insights

## Performance Observations

## Spark Web UI

![]("./prob1_UI.png)

## Explanation of Problem 2 Visualizations