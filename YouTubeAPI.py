import math
import json
from urllib.error import HTTPError
from googleapiclient.discovery import build

def call(file_name = "MrBeast", user_name = "MrBeast6000"):

	# Build the YouTube search service
	service = build("youtube", "v3", developerKey = "AIzaSyCtL1EJnBSOpsSfr0hPZAzbEJl9SXuMtmI")

	# We need to store the data retrieved from the API
	file_path = "C:\\data\youtube_api_data\\" + file_name + ".txt"
	file_mode = "w"
	file_encoding = "utf-8"

	### Querying the YouTube API to get a Channel's 'uploads' playlist ID ###

	# The maximum number of elements per page of response
	# is capped at 50 per the YouTube API documentation
	results_per_page = 50

	# Setting what content to retrieve from the API
	channel_name = user_name
	channel_part = "contentDetails,statistics"
	channel_request = service.channels().list(part = channel_part, forUsername = channel_name)

	# Execute the request
	try:
		channel_response = channel_request.execute()
	except HTTPError as e:
		print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))


	### Querying the API to get the channel's uploaded video IDs ###

	# Setting what content to retrieve from the API
	playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
	playlist_part = "snippet"
	playlist_page_token = ""

	# We need to store each video ID so we can search it later
	video_ids = []

	# We need to know how many times to make the request
	video_count = int(channel_response['items'][0]['statistics']['videoCount'])
	page_count = math.ceil(video_count / results_per_page)
	
	# Execute the new request
	try:
		# Process each page of the response
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
			if(i != (page_count - 1)):
				playlist_page_token = playlist_response['nextPageToken']

			# Add a new element to the string array
			video_ids.append("")

			# Add each video ID to a string array (needed when searching for videos)
			for video_element in playlist_response['items']:
				video_ids[i] += video_element['snippet']['resourceId']['videoId']
				video_ids[i] += ","

			# Remove the trailing comma
			video_ids[i] = video_ids[i][:-1]

	except HTTPError as e:
		print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))


	### Querying the API to get the channel's uploaded video IDs ###

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

		except HTTPError as e:
			print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))

	finally:
		output_file.close()

if __name__ == "__main__":
	call()