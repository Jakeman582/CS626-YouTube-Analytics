import math
import json
import sys
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def call(file_name = "MrBeast", user_name = "MrBeast6000"):

	# Build the YouTube search service
	service = build("youtube", "v3", developerKey = "AIzaSyCtL1EJnBSOpsSfr0hPZAzbEJl9SXuMtmI")

	# We need to store the data retrieved from the API
	file_path = "C:\\\\data\\youtube_api_data\\" + file_name + ".txt"
	file_mode = "w"
	file_encoding = "utf-8"

	#########################################################################
	### Querying the YouTube API to get a Channel's 'uploads' playlist ID ###
	#########################################################################

	# LOGGING: Print the username whose videos we are getting
	print("Fetching \"uploads\" playlist ID for " + file_name)

	# The maximum number of elements per page of response
	# is capped at 50 per the YouTube API documentation
	results_per_page = 50

	# Setting what content to retrieve from the API
	channel_name = user_name
	channel_part = "contentDetails,statistics"

	# If the first two characters are "UC", then it's a channel ID, not a username
	if(user_name.startswith("UC")):
		channel_request = service.channels().list(part = channel_part, id = channel_name)
	else:
		channel_request = service.channels().list(part = channel_part, forUsername = channel_name)

	# Execute the request
	try:
		channel_response = channel_request.execute()

		# LOGGING: Fetched the 'uploads' playlist
		print("Fetched \"uploads\" playlist ID for " + file_name)

	except HttpError as e:
		print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))
		sys.exit(0)


	################################################################
	### Querying the API to get the channel's uploaded video IDs ###
	################################################################

	# LOGGING: Querying the API to get the uploaded video IDs
	print("Fetching video IDs for " + file_name)

	# Setting what content to retrieve from the API
	playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
	playlist_part = "snippet"
	playlist_page_token = ""

	# We need to store each video ID so we can search it later
	video_id_set = set()

	# We need to know how many times to make the request
	video_count = int(channel_response['items'][0]['statistics']['videoCount'])
	page_count = math.ceil(video_count / results_per_page)
	
	# Execute the new request
	try:

		# process each page of the response
		for i in range(page_count):

			# Construct and execute the request
			playlist_request = service.playlistItems().list(
				part = playlist_part, 
				playlistId = playlist_id,
				pageToken = playlist_page_token,
				maxResults = results_per_page
			)
			playlist_response = playlist_request.execute()

			# Update the page token if not on the last page
			if('nextPageToken' in playlist_response):
				if(playlist_page_token != playlist_response['nextPageToken']):
					playlist_page_token = playlist_response['nextPageToken']

			# Add each video id to a string array (needed when searching for videos)
			for video_element in playlist_response['items']:
				current_video = video_element['snippet']['resourceId']['videoId']

				if(current_video not in video_id_set):
					video_id_set.add(current_video)

		# DEBUG: CHECK TO SEE IF CNN PRODUCED 151282 VIDEO IDS
		print("Fetched video IDs for " + file_name)

	except HttpError as e:
		print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))

	except KeyError as e:
		print("" + playlist_response)

	#####################################################################
	### Querying the API to get the channel's uploaded video metadata ###
	#####################################################################

	# Logging: Getting metadata for each video
	print("Fetching video metadata for " + file_name)

	# Split the set of video ids into groups of 50 to reduce quota strains
	video_ids = []
	count = 0
	id_string = ""
	for id in video_id_set:
		id_string += id
		id_string += ","
		count += 1
		if(count >= results_per_page):
			id_string = id_string[:-1]
			video_ids.append(id_string)
			id_string = ""
			count = 0

	# Take care of any partially full pages not handled by the above loop
	if(count != 0):
		id_string = id_string[:-1]
		video_ids.append(id_string)


	# Setting what content to get from the API
	video_part = "snippet,statistics"
	video_page_token = ""

	# Execute the request
	try:
		# Open the file so we can store the data
		output_file = open(file = file_path, mode = file_mode, encoding = file_encoding)
		
		# Execute the request
		try:
			# Process each page of the response
			for id_string in video_ids:
				# Construct and execute the request
				video_request = service.videos().list(
					part = video_part,
					id = id_string
				)
				video_response = video_request.execute()

				# For now, display the results on the screen, save to a file later
				for video in video_response['items']:

					# Check if views are enabled
					view_count = -1
					if(video['statistics'].get('viewCount')):
						view_count = int(video['statistics']['viewCount'])

					# Check if likes are enabled
					like_count = -1
					if(video['statistics'].get('likeCount')):
						like_count = int(video['statistics']['likeCount'])

					# Check if dislikeslikes are enabled
					dislike_count = -1
					if(video['statistics'].get('dislikeCount')):
						dislike_count = int(video['statistics']['dislikeCount'])

					# Check if comments are enabled
					comment_count = -1
					if(video['statistics'].get('commentCount')):
						comment_count = int(video['statistics']['commentCount'])

					# Print the response for now, save in file later
					print(
						"{0} | {1} | {2} | {3} | {4}".format(
							view_count, 
							like_count, 
							dislike_count, 
							comment_count,
							video['snippet']['title'], 
						),
						file = output_file
					)

			# LOGGING: FEtched video metadata
			print("Fetched video metadata for " + file_name)

		except HttpError as e:
			print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))

	finally:
		output_file.close()

if __name__ == "__main__":
	call()