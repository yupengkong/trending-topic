import requests
import time
import math
import psycopg2
import random
import datetime
import pandas as pd


def initialization(startDate, endDate):  # initilize the time window and generate an empty dictionary for data storage

	minTime, maxTime = time.mktime(time.strptime(startDate, "%Y-%m-%d")), time.mktime(time.strptime(endDate, "%Y-%m-%d"))
	timeRange = (maxTime - minTime)/86400

	d = {i:[] for i in range(1, int(timeRange) + 1)}

	return minTime, maxTime, timeRange, d

def epochToString(timestamp):

	return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')


def getVideosForTopics(topic = '/m/04ndr', limit = 8000, startDate = '2015-06-15', endDate = '2015-06-21'): # query all the videos published in the given time window under a topic 
	
	minTime, maxTime, window, d = initialization(startDate, endDate) # convert to epoch time, and initialize a dictionary to store video ids

	video_category = {} # initialize a dictionary to store the video with its category

	baseURL = "http://api.channelmeter.com/v3/youtube/videos/rank" # use the 'rank' method to query the API, will return the videos under the same topic

	paramDict = {
			"topic" : topic,   # specify the topic_id from freebase
			"limit" : limit,   # usually set a very high limit to make sure that all the videos published in the given time window will return, usually 4000 to 5000 for a week
			"sort" : "views",  # use 'sort' option to make sure that we will not omit the important videos based on the number of views
			"pub-after" : startDate,  # specify the time-window published after and before
			"pub-before" : endDate
	}   

	flag = True   # set a flag in case of any exceptions during connection with API                        

	while flag:

		try:
			r = requests.get(baseURL, params = paramDict) # query the API with the parameters specified above
			temp = r.json() # get the data from API

			if temp: 
				for data in temp:
					d[int((data['published'] - minTime) /86400) + 1].append(data['id'])  # the format of dictionary {1:[video ids], 2: [video ids]....}, categorize videos into different days with the given time-window
					video_category[data['id']] = data['category'] # 
			else:
				d = {} # if no data return, keep the dictioary empty

			flag = False # reset flag to jump out of the loop

		except requests.exceptions.RequestException: # if the API refuses the connection, then sleep and wait a little bit before attempting to connect again
			time.sleep(20) 
			continue

		except Exception as e: # for all other kinds of exception, an empty dictionary will be returned and processed further in next step
			d = {}
			flag = False

	return d, video_category  # should return a dictionary, the key is 1, 2, 3, 4,, ... representing first day, second day,...
	                          # the value is a list, including all the video ids published in the first day, second day, ....
	                          # 
	                          # also return another video category dictionary, the key is the video_id, and the value is the corresponding category


def getVideoViews(videolist, startDate = '2015-05-04', endDate = '2015-05-10',  metric = 'views', group = 'day'): # Based on the video_id queried via the function of getVideosForTopics, this function will return the daily views for each video

	baseURL = "http://api.channelmeter.com/v3/youtube/query"
	minTime, maxTime, window, d = initialization(startDate, endDate) # initialize a dictionary to store the total views of all videos in a given day

	endDate = epochToString(maxTime-86400)

	video_views = {video : [] for video in videolist} # initialize a dictionary, the key is the video_id, the value is the number of daily views for this video in the given time window

	for video in videolist:  # a loop to traverse all the videos 

		paramDict = {
				"id":"video:" + video,   # query each video
				"metric" : metric,       # use 'views' as metric
				"group" : group,         # use 'day'
				"start" : startDate,     # specify startDate, and endDate, here 'startDate' is the same as 'pub-after' used above, however, 'endDate' is NOT 
				"end" : endDate          # the same as 'pub-before', there are one day difference between 'pub-before' and 'endDate'
		}

		flag = True  # set a flag to handle exceptions during API query

		while flag:

			try:
				r = requests.get(baseURL, params = paramDict)  # get the data from API
				temp = r.json()

				for epoch, views in temp['data']: # traverse the daily-view based data for a video

					d[int((epoch[0] - minTime) / 86400.0) + 1].append(views[0])  # get views for all videos on each day in the given time window
					video_views[video].append((int((epoch[0] - minTime)/ 86400.0) + 1, views[0])) # for each video, get its daily views as a tuple (1, 1000), (3, 4000)...

				flag = False # reset the flag

			except requests.exceptions.RequestException: # handle the connection exception
				time.sleep(20)
				continue

			except Exception as e:  # handle all other exceptions
				flag = False



	return video_views, d     # return a list of video_views for each video, each element of this list is a tuple, first tuple element is the day
	                          # second tuple element is the number of views in that day e.g.  {'L6zjhUWj65U: [(6,10), (2,34), ...]', ....}
	                          # 

	# return a dictionary d, the key is 1, 2, 3, 4, ..., representing first day, second day, ...
	# the value is a list, each elements indicates the number of views for a video in that day

def getTotal(topics, startDate = '2015-05-04', endDate = '2015-05-10'):

	ddata = {}

	for topic in topics:   # traverse all the topics for the purpose of trending test

		para1 = [topic, 1000, startDate, endDate]
		videos, video_category = getVideosForTopics(*para1)   # get all the videos in a dictionary as the form {1:[...], 2: [...],...}
                                                              # key is the first day, second day, .., the value is a list of video ids published on that day
		if not videos:      # if video dictionary is empty, then skip
			continue
		else:
			videolist = [video for meta in [videos[key] for key in videos] for video in meta] # generate the video list for a topic, all videos are published during specified time window

			para2 = [videolist, startDate, endDate]
			video_views, data = getVideoViews(*para2)  # get a dictionary for views of each video on each day, 
			
			
			views = [sum(data[key]) for key in sorted(data)]        # generate two list, one for daily views of all videos with the same topic  
			uploads = [len(videos[key]) for key in sorted(videos)]  # the other for daily number of video uploads for the same topic			
			
			ddata[topic] = views
			print topic, views, uploads
			'''
			mean_view = viewCompare(video_views, int(initialization(startDate, endDate)[2]))    # the other is the number of videos uploads on each day, elements in these two lists are stored as the sequence of day
			
		dict3 = {key:[video_category[key], mean_view[key]] for key in video_category}

		#print sorted(dict3.items(), key = lambda x : x[1][1], reverse = True)

			#insertion(topic, views, uploads)
			#print video_views
		'''	

	return ddata

def viewCompare(video_views, window):  # return the top 20 videos, which have the highest average views in the latter half of time window
	
	mean_views = {}

	for key in video_views:
		test_views = [views for day, views in video_views[key] if day >= window / 2]
		mean_views[key] = avg(test_views) if test_views != [] else 0

	return mean_views

 
def avg(vec):    
	return sum(vec) / float(len(vec))

def std(vec):     # calculate standard deviation

	mean = avg(vec)
	return math.sqrt(1.0/(len(vec)-1) * sum([(x-mean)*(x-mean) for x in vec])) if len(vec) > 1 else 0


def mean_std(data, window):  # calculate the average, standard deviation based on the all topics(globalInfo) and individual topic(localInfo)

	globalInfo = []
	localInfo = {}

	for key in data:
		globalInfo.extend(data[key][:window/2])   # put first half of the views of all topics into globalInfo 
		localInfo[key] = [avg(data[key][:window/2]), std(data[key][:window/2])] # for the first half of time window, calculate average and standard deviation for each topic

	globalInfo = [avg(globalInfo), std(globalInfo)] # global average, global standard deviation are calculated based on all the topics
	return globalInfo, localInfo


def Z_score(data, globalInfo, localInfo, window): # calculate both global Z-score and local Z-score for each topic

	record = pd.DataFrame(columns = ['topic_id', 'GlobalZ', 'LocalZ'])
	
	for key in localInfo:  # consider each topic

		try:

			globalZ = avg([(x - globalInfo[0]) / float(globalInfo[1]) for x in data[key][window/2 : ]])    # calculate the average global Z-score based on the views from the second half time window
			localZ = avg([(x - localInfo[key][0]) / float(localInfo[key][1]) for x in data[key][window/2 : ]]) # calculate the average local Z-score based on the views from the second half time window
                                                                # global Z-score is based on global mean and global standard deviation (calculated from all topics)
            
			record.loc[len(record)] = [key, globalZ, localZ]       # local Z-score is based on local mean and local standard deviation (calculated from single topic)
			#print key, globalZ, localZ
		except Exception as e: # handle all other exceptions
			print str(e)
			continue

	return record


def topics_list():  # read from database to get a list of topics which will be considered in the trending topic calculation (as topics pool)
                    # This is still an open question, how can we limit the topics we want to consider, because there are 48 millions topics in free base
	global cur
	global conn

	query = "SELECT * from freebasetopics";

	try:
		cur.execute(query)
		rows = cur.fetchall()

	except Exception:
		print 'unsuccessful topic query'
		pass

	data = pd.DataFrame([('/m/' + a, b) for (a, b) in rows], columns = ['topic_id', 'topic'])

	return data


if __name__ == "__main__":  

	startDate = '2015-06-01'  # specify the time window, so far, I only consider 7 days(a week), need to change the database schema
	endDate = '2015-07-01'    # if consider different length of time window

	windows = int(initialization(startDate, endDate)[2])

	cur, conn = databaseConn()

	topicslist = topics_list()


	topics = list(topicslist['topic_id'])  # get the topics you want to consider for trending topic calculation

	data = getTotal(topics, startDate, endDate) # query video views, and number of videos upload, and save it into database
	

	data = {key : data[key] for key in data if avg(data[key][:windows/2]) > 0}
	globalInfo, localInfo = mean_std(data, windows)
	Zscore = Z_score(data, globalInfo, localInfo, windows)

	conn.close()
	cur.close()

	#Zscore = {topicslist[key]: value for key, value in Zscore.items()}

	newdata = pd.merge(topicslist, Zscore, on='topic_id')
	newdata = pd.DataFrame(newdata, columns = ['topic', 'GlobalZ', 'LocalZ'])
	final1 = newdata[(newdata['GlobalZ'] > 0) & (newdata['LocalZ'] > 0)].sort(['GlobalZ', 'LocalZ'], ascending = [0, 0])
	final2 = newdata[newdata['GlobalZ'] * newdata['LocalZ'] < 0].sort('LocalZ', ascending = 0)
	final3 = newdata[(newdata['GlobalZ'] < 0) & (newdata['LocalZ'] < 0)].sort(['GlobalZ', 'LocalZ'], ascending = [0, 0])

	final = pd.concat([final1, final2, final3])
	#print final
	final.to_csv('output2.csv')


