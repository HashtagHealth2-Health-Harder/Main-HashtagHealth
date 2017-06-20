# -*- coding: utf-8 -*-
"""
	Twitter Streamer for HashtagHealth.
	Branched from health_stream.py, inserts directly to database
	instead of saving to file

	Created by Gabby 'newfound strawberry lover' Ortman. 
"""

import tweepy as ty # Twitter API wrapper
import pickle
import sys
import time
from keys import *
import populate_database
# override tweepy.StreamhListener to create HashtagHealth stream listener.
# http://docs.tweepy.org/en/v3.4.0/streaming_how_to.html
class HashtagHealthListener(ty.StreamListener): 
	"""
		Twitter Streamer for HashtagHealth
	"""
	def __init__(self, api, category):
		self.api = api
		self.category = category
		super(ty.StreamListener, self).__init__()
        
	def on_status(self, status): 
		timestamp = '{}:{}'.format(status.created_at, status.timestamp_ms)
		tweet_id = status.id
		# if status has a location
		if(status.place):
			# and it's in the united states
			if(status.place.country_code == 'US'):
				self.populate_data(category, status)	

	def on_error(self, error): 
		# possible errors:
			# unicode(?)
			# rate limit
		print(error)
		time.sleep(60 * 15)

	def populate_data(self, status, category): 
		populate_database.populate_data(category, status)

def set_twitter_auth(): 
	"""
		Authorize via Twitter API
	"""
	auth = ty.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
	api = ty.API(auth, wait_on_rate_limit= True, wait_on_rate_limit_notify= True)
	return api

if __name__ == '__main__':
	if(len(sys.argv) >= 2):
		api = set_twitter_auth(); 
		category = sys.argv[1]
		health_listener = HashtagHealthListener(api, category)	
		health_stream = ty.Stream(auth = api.auth, listener = health_listener)
		track_list = list(sys.argv[2::])
		health_stream.filter(track=track_list) #lists of identifiers