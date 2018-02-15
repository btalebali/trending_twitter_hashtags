This repository contains three application.
A simple application that reads online streams from Twitter using Python,
A seconde Spark Streaming application that processes the tweets to identify hashtags
The last application returns top trending hashtags and represents this data on a real-time dashboard.



# Twitter HTTP Client
It's a a simple client that will get the tweets from Twitter API using Python 
and passes them to the Spark Streaming instance.
A twitter app is used to get tweets for Twitter.


# Apache Spark Streaming Application

The Spark streaming app that will do real-time processing for the incoming tweets, extract the hashtags from them, and calculate how many hashtags have been mentioned,

First, we have to create an instance of Spark Context sc, then we created the Streaming Context ssc from sc with a batch interval two seconds that will do the transformation on all streams received every two seconds. Notice we have set the log level to ERROR in order to disable most of the logs that Spark writes.

We defined a checkpoint here in order to allow periodic RDD checkpointing; this is mandatory to be used in our app, as we’ll use stateful transformations (will be discussed later in the same section).

Then we define our main DStream dataStream that will connect to the socket server we created before on port 9009 and read the tweets from that port. Each record in the DStream will be a tweet.


Then, we’ll define our transformation logic. First we’ll split all the tweets into words and put them in words RDD. Then we’ll filter only hashtags from all words and map them to pair of (hashtag, 1) and put them in hashtags RDD.

Then we need to calculate how many times the hashtag has been mentioned. We can do that by using the function reduceByKey. This function will calculate how many times the hashtag has been mentioned per each batch, i.e. it will reset the counts in each batch.

In our case, we need to calculate the counts across all the batches, so we’ll use another function called updateStateByKey, as this function allows you to maintain the state of RDD while updating it with new data. This way is called Stateful Transformation.

Note that in order to use updateStateByKey, you’ve got to configure a checkpoint, and that what we have done in the previous step.


The updateStateByKey takes a function as a parameter called the update function. It runs on each item in RDD and does the desired logic.

In our case, we’ve created an update function called aggregate_tags_count that will sum all the new_values for each hashtag and add them to the total_sum that is the sum across all the batches and save the data into tags_totals RDD.



Then we do processing on tags_totals RDD in every batch in order to convert it to temp table using Spark SQL Context and then perform a select statement in order to retrieve the top ten hashtags with their counts and put them into hashtag_counts_df data frame.



The last step in our Spark application is to send the hashtag_counts_df data frame to the dashboard application. So we’ll convert the data frame into two arrays, one for the hashtags and the other for their counts. Then we’ll send them to the dashboard application through the REST API.


Finally, here is a sample output of the Spark Streaming while running and printing the hashtag_counts_df, you’ll notice that the output is printed exactly every two seconds as per the batch intervals.



# Simple Real-time Dashboard for Representing the Data

using Python, Flask, and Charts.js.



Then, in the app.py file, we’ll create a function called update_data, which will be called by Spark through the URL http://localhost:5001/updateData in order to update the Global labels and values arrays.

Also, the function refresh_graph_data is created to be called by AJAX request to return the new updated labels and values arrays as JSON. The function get_chart_page will render the chart.html page when called.






Now, let’s create a simple chart in the chart.html file in order to display the hashtag data and update them in real time. As defined below, we need to import the Chart.js and jquery.min.js JavaScript libraries.

In the body tag, we have to create a canvas and give it an ID in order to reference it while displaying the chart using JavaScript in the next step.




Now, let’s construct the chart using the JavaScript code below. First, we get the canvas element, and then we create a new chart object and pass the canvas element to it and define its data object like below.

Note that the data’s labels and data are bounded with labels and values variables that are returned while rendering the page when calling a get_chart_page function in the app.py file.

The last remaining part is the function that is configured to do an Ajax request every second and call the URL /refreshData, which will execute refresh_graph_data in app.py and return the new updated data, and then update the char that renders the new data.




# Running the applications together

Let’s run the three applications in the order below: 

twitter_app.py, spark_stream_app.py and HashtagDashboard


Then you can access the real-time dashboard using the URL <http://ec2-35-178-24-0.eu-west-2.compute.amazonaws.com:5001/>







