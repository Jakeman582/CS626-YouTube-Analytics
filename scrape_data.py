import sys
import YouTubeAPI

def run(input_file = ""):
	
	# If no file is supplied, call the default call method for the YouTube API
	if(input_file == ""):
		YouTubeAPI.call()
	else:
		# Here, we need to get the username and filename from the input file
		with open(file = input_file, mode = "r", encoding = "utf-8") as input:
			input_lines = input.readlines()
			for line in input_lines:
				line.strip()
				filename, username = line.split(",")
				YouTubeAPI.call(filename, username)

if __name__ == "__main__":
	if(len(sys.argv) <= 1):
		run()
	else:
		run(sys.argv[1])