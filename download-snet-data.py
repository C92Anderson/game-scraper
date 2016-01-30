import urllib
import json
import re
from bs4 import BeautifulSoup
from pprint import pprint

# Load the web page
snetGameId = 1554021
web = urllib.urlopen("http://www.sportsnet.ca/hockey/nhl/livetracker/game/" + str(snetGameId))
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

for play in plays:
	pprint(play)

#pprint(plays)
#print gameId
