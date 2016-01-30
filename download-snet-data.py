import urllib
import json
import re
import sys
from bs4 import BeautifulSoup
from pprint import pprint

# Configure where snet data is stored
season = 20152016
outDir = "snet-data/" + str(season) + "/"

# Get date argument: "yyyy-mm-dd"
dateArg = sys.argv[1]

#
#
# Get list of games played on the specified date
# The list will contain a list of snet gameIds
#
#

snetIds = []

url = "http://www.sportsnet.ca/hockey/nhl/scores/?datepicker-date=" + dateArg
web = urllib.urlopen(url)
soup = BeautifulSoup(web.read(), "lxml")

# Get all game cards
els = soup.find_all("div", {"class":"game-card-container"})
for el in els:
	idPrefix = "game_card_container_"
	snetId = el["id"][el["id"].find(idPrefix) + len(idPrefix):]
	snetIds.append(int(snetId))

print("GETTING DATA FOR SNET IDS: " + str(snetIds))

#
#
# For each of the snet gameIds, get the game page and get the nhl gameId and the event json
#
#

for sId in snetIds:

	# Load the web page
	web = urllib.urlopen("http://www.sportsnet.ca/hockey/nhl/livetracker/game/" + str(sId))
	soup = BeautifulSoup(web.read(), "lxml")

	# Get the 3rd script element, which contains the json
	script = soup.find_all("script")[2].string

	# Find the value assigned to 'bootstrap' by getting the contents between 'bootstrap = ' and ';'
	# pattern.findall returns a string
	pattern = re.compile('(?<=bootstrap\s=)(.*)(?=;)')
	jsonStr = pattern.findall(script)[0]

	# Parse the jsonStr string as a json
	jsonDict = json.loads(jsonStr)

	# Get the NHL gameId and the plays json
	gameId = ""
	plays = dict()
	for key, value in jsonDict.items():
		if key == "game":
			gameId = value["id"]
		elif key == "plays":
			plays = value

	# Write events json to a file and use the nhl gameId as the filename
	outFile = open(outDir + "snet-" + str(gameId) + ".json", "w")
	outFile.write(json.dumps(plays))
	outFile.close()