import json
from googleapiclient.discovery import build

def main():

	# Build the YouTube search service
	service = build("youtube", "v3", developerKey = "AIzaSyCtL1EJnBSOpsSfr0hPZAzbEJl9SXuMtmI")

	# Search for the most popular videos on YouTube
	request = service.videos().list(part = "snippet,statistics,topicDetails", chart = "mostPopular")

	# Execute the request
	try:
		response = request.execute()
		print(json.dumps(response, sort_keys = True, indent = 4))
	except HttpError as e:
		print("Error response status code: {0}, reason: {1}".format(e.resp.status, e.error_details))

if __name__ == "__main__":
	main()