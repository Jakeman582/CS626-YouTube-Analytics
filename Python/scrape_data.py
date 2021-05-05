import sys
from pathlib import Path

import YouTubeAPI

def check_present_files(data_directory):

	path = Path(data_directory)

	# Get each YouTuber's file from the directory
	return [file.stem for file in path.iterdir() if file.is_file()]


def run(input_file = ""):
	
	# If no file is suplied, call the default call method for the YouTube API
	if(input_file == ""):
		YouTubeAPI.call()

	# Here, we need to get the username and filename from the input file
	else:

		# Make sure we only put in files for YouTubers we have not 
		# retrieved data for, which should help ease quota usage
		files = check_present_files("C:\\\\data\\youtube_api_data\\")
		print(files)
		
		with open(file = input_file, mode = "r", encoding = "utf-8") as input:
			input_lines = input.readlines()
			for line in input_lines:

				# We can ignore lines that start with #, and any empty lines
				#if(not line.startswith("#") and length(line) != 0):
				if(not line.startswith("#")):

					# Get the common name and user name for each YouTuber
					line.strip()
					file_name, user_name = line.split(",")

					# Make sure we haven't yet processed this YouTuber's
					# channel
					if(file_name not in files):
						YouTubeAPI.call(file_name, user_name)

if __name__ == "__main__":
	if(len(sys.argv) <= 1):
		run()
	else:
		run(sys.argv[1])