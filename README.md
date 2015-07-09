This project is to rank trending topics based on video views. The steps are listed below
1. query all the videos (video-id) with the same topic published in a specific time window ( e.g, one week)
2. query the number of view for each video in each day during the specific time window
3. get the total number views of videos for one topic
4. calculate global Z-score and local Z-score to rank the topic

the output is a csv file which contains 4 columns, rank, topic, global Z-score and local Z-score
