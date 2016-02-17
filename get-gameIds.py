import sys
import urllib2
import json
from pprint import pprint
from operator import itemgetter

#
# 
# Get user argument and create the json location
#
#

dateArg = sys.argv[1]

if len(dateArg) != 8:
	print "Enter a date in yyyymmdd format"
	sys.exit()

yearArg = dateArg[0:4]
monthArg = dateArg[4:6]
dayArg = dateArg[6:]

if int(yearArg) > 2020 or int(yearArg) < 2010 or int(monthArg) < 1 or int(monthArg) > 12 or int(dayArg) < 1 or int(dayArg) > 31:
	print "Enter a date in yyyymmdd format"
	sys.exit()

requestStr = yearArg + "-" + monthArg + "-" + dayArg
jsonLoc = "https://statsapi.web.nhl.com/api/v1/schedule?startDate=" + requestStr + "&endDate=" + requestStr
print jsonLoc

#
#
# Get json
#
#

response = urllib2.urlopen(jsonLoc)
html = response.read()
jsonDict = json.loads(html)
dates = jsonDict["dates"]

#
#
# Get gameIds from json
#
#

outGames = []
for date in dates:
	for game in date["games"]:
		gameDict = dict()
		gameDict["gameId"] = game["gamePk"]
		gameDict["state"] = game["status"]["detailedState"]
		outGames.append(gameDict)

outGames = sorted(outGames, key=itemgetter("gameId"))

#
#
# Print gameIds
#
#

print " "
print requestStr
print "SEASON  GAMEID" 
for game in outGames:
	print str(game["gameId"])[0:4] + "    " + str(game["gameId"])[5:] + "    " + game["state"] 
print " "