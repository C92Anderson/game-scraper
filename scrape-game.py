# For scraping and processing raw data, and creating csv
import sys
import urllib
import os.path
import json
import copy
import re
from pprint import pprint

# For loading csv files into database
import mysql.connector
import dbconfig

# Take string "mm:ss" and return the number of seconds (as an integer)
def toSecs(timeStr):
	mm = int(timeStr[0:timeStr.find(":")])
	ss = int(timeStr[timeStr.find(":")+1:])
	return 60 * mm + ss

# Check if the key k is in the dictionary d
# If it isn't, return NULL
# If it exists, return the key's value as a string
def outputVal(d, k):
	if k not in d:
		return "NULL"
	else:
		return str(d[k])

#
# 
# Get user arguments
#
#

seasonArg = int(sys.argv[1])					# Specify 20142015 for the 2014-2015 season
shortSeasonArg =  int(str(seasonArg)[0:4])		# The starting year of the season
gameArg = str(sys.argv[2])						# Specify a gameId 20100, or a range 20100-20105
gameIds = []									# List of gameIds to scrape

inDir = "nhl-data/"								# Where the input files are stored
outDir = "data-for-db/"							# Where the output files (to be written to database) are stored

# Convert gameArg into a list of gameIds
if gameArg.find("-") > 0:
	startId = int(gameArg[0:gameArg.find("-")])
	endId = int(gameArg[gameArg.find("-") + 1:])
	for gameId in range(startId, endId + 1):
		gameIds.append(int(gameId))
else:
	gameIds = [int(gameArg)]

#
#
# Scrape data for each game
#
#

# Converts full team names used in json (e.g., the event team) to json abbreviations (e.g., sjs)
teamAbbrevs = dict()	
teamAbbrevs["carolina hurricanes"] = "car"
teamAbbrevs["columbus blue jackets"] = "cbj"
teamAbbrevs["new jersey devils"] = "njd"
teamAbbrevs["new york islanders"] = "nyi"
teamAbbrevs["new york rangers"] = "nyr"
teamAbbrevs["philadelphia flyers"] = "phi"
teamAbbrevs["pittsburgh penguins"] = "pit"
teamAbbrevs["washington capitals"] = "wsh"
teamAbbrevs["boston bruins"] = "bos"
teamAbbrevs["buffalo sabres"] = "buf"
teamAbbrevs["detroit red wings"] = "det"
teamAbbrevs["florida panthers"] = "fla"
teamAbbrevs["montreal canadiens"] = "mtl"
teamAbbrevs["ottawa senators"] = "ott"
teamAbbrevs["tampa bay lightning"] = "tbl"
teamAbbrevs["toronto maple leafs"] = "tor"
teamAbbrevs["chicago blackhawks"] = "chi"
teamAbbrevs["colorado avalanche"] = "col"
teamAbbrevs["dallas stars"] = "dal"
teamAbbrevs["minnesota wild"] = "min"
teamAbbrevs["nashville predators"] = "nsh"
teamAbbrevs["st. louis blues"] = "stl"
teamAbbrevs["winnipeg jets"] = "wpg"
teamAbbrevs["anaheim ducks"] = "ana"
teamAbbrevs["arizona coyotes"] = "ari"
teamAbbrevs["calgary flames"] = "cgy"
teamAbbrevs["edmonton oilers"] = "edm"
teamAbbrevs["los angeles kings"] = "lak"
teamAbbrevs["san jose sharks"] = "sjs"
teamAbbrevs["vancouver canucks"] = "van"

#
# Situations and stats to record
#

scoreSits = [-3, -2, -1, 0, 1, 2, 3]
strengthSits = ["ownGPulled", "oppGPulled", "sh45", "sh35", "sh34", "pp54", "pp53", "pp43", "ev5", "ev4", "ev3", "other"]
teamStats = ["toi", "gf", "ga", "sf", "sa", "bsf", "bsa", "msf", "msa", "foWon", "foLost", "ofo", "dfo", "nfo", "penTaken", "penDrawn"]
playerStats = ["toi", "ig", "is", "ibs", "ims", "ia1", "ia2", "blocked", "gf", "ga", "sf", "sa", "bsf", "bsa", "msf", "msa", "foWon", "foLost", "ofo", "dfo", "nfo", "penTaken", "penDrawn"]

# foWon: team won face-offs, individually won face-offs
# foLost: team lost face-offs, individually lost face-offs
# ig, is, ibs, ims, ia1, ia2: individual goals, shots, blocked shots, missed shots, primary assists, secondary assists
# blocked: shots blocked by the individual

for gameId in gameIds:

	if gameId < 20000 or gameId >= 40000:
		print "Invalid gameId: " + str(gameId)
		continue

	print "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
	print "Processing game " + str(gameId)

	# Dictionaries to store data from the input json
	players = dict()
	teams = dict()
	events = dict()

	# Dictionaries for output
	gameDate = 0
	outPlayers = dict()
	outTeams = dict()
	outEvents = []

	#
	#
	# Download input files
	#
	#

	# Input file urls
	shiftJsonUrl = "http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=" + str(shortSeasonArg) + "0" + str(gameId)
	pbpJsonUrl = "https://statsapi.web.nhl.com/api/v1/game/" + str(shortSeasonArg) + "0" + str(gameId) + "/feed/live"

	# Downloaded input file names
	shiftJson = str(seasonArg) + "-" + str(gameId) + "-shifts.json"
	pbpJson = str(seasonArg) + "-" + str(gameId) + "-events.json"

	# Download files that don't already exist
	filenames = [shiftJson, pbpJson]
	fileUrls = [shiftJsonUrl, pbpJsonUrl]
	for i, filename in enumerate(filenames):
		if os.path.isfile(inDir + filename) == False:
			print "Downloading " + str(filename)
			urllib.urlretrieve(fileUrls[i], inDir + filename)
		else:
			print str(filename) + " already exists"

	print "- - - - -"

	#
	#
	# Parse pbpJson
	#
	#

	inFile = file(inDir + pbpJson, "r")
	inString = inFile.read()
	jsonDict = json.loads(inString)
	inFile.close()

	gameDate = jsonDict["gameData"]["datetime"]["dateTime"]
	gameDate = int(gameDate[0:10].replace("-", ""))					# Convert from dateTime format to an int (of the date)
	players = copy.deepcopy(jsonDict["gameData"]["players"])		# Keys: 'ID#' where # is a playerId
	teams = copy.deepcopy(jsonDict["gameData"]["teams"])			# Keys: 'home', 'away'
	events = copy.deepcopy(jsonDict["liveData"]["plays"]["allPlays"])
	rosters = copy.deepcopy(jsonDict["liveData"]["boxscore"]["teams"])
	linescore = copy.deepcopy(jsonDict["liveData"]["linescore"])
	jsonDict.clear()

	# Reformat the keys in the 'players' dictionary: from 'ID#' to # (as an int), where # is the playerId
	# We're going to use the players dictionary to get player positions and names
	tempPlayers = dict()
	for pId in players:
		newKey = int(pId[2:])
		tempPlayers[newKey] = players[pId]
	players = copy.deepcopy(tempPlayers)
	tempPlayers.clear()

	#
	#
	# Prepare team output
	#
	#

	for iceSit in teams:	# iceSit = 'home' or 'away'

		outTeams[iceSit] = dict()
		outTeams[iceSit]["abbrev"] = teams[iceSit]["abbreviation"].lower()	# team name abbreviation

		# Initialize stats
		for strSit in strengthSits:
			outTeams[iceSit][strSit] = dict()
			for scSit in scoreSits:
				outTeams[iceSit][strSit][scSit] = dict()
				for stat in teamStats:
					outTeams[iceSit][strSit][scSit][stat] = 0

	#
	#
	# Prepare players output
	#
	#

	# Remove inactive players from the boxscore's player's list
	# In the pbp json for 2015020002, both Stoll and Etem have #26, but Etem has no stats and doesn't appear in any events
	for iceSit in rosters:							# 'iceSit' will be 'home' or 'away'
		for player in rosters[iceSit]["players"]:	# 'player' will be 'ID#' where # is a playerId
			if "stats" not in rosters[iceSit]["players"][player] or len(rosters[iceSit]["players"][player]["stats"]) == 0:
				del players[(int(rosters[iceSit]["players"][player]["person"]["id"]))]	# Remove the inactive player from the 'players' dictionary

	# Prepare the output dictionary outPlayers
	for pId in players:
		outPlayers[pId] = dict()
		outPlayers[pId]["position"] = players[pId]["primaryPosition"]["abbreviation"].lower()
		outPlayers[pId]["firstName"] = players[pId]["firstName"]
		outPlayers[pId]["lastName"] = players[pId]["lastName"]

		# Get the player's team, iceSit, and jersey number
		for iceSit in rosters:	# 'iceSit' will be 'home' or 'away'
			rosterKey = "ID" + str(pId)
			if rosterKey in rosters[iceSit]["players"]:
				outPlayers[pId]["team"] = outTeams[iceSit]["abbrev"]
				outPlayers[pId]["iceSit"] = iceSit
				outPlayers[pId]["jersey"] = rosters[iceSit]["players"][rosterKey]["jerseyNumber"]

		# Initialize stats
		for strSit in strengthSits:
			outPlayers[pId][strSit] = dict()
			for scSit in scoreSits:
				outPlayers[pId][strSit][scSit] = dict()
				for stat in playerStats:
					outPlayers[pId][strSit][scSit][stat] = 0

	#
	#
	# Prepare events output
	#
	#

	print "Processing json events"

	# Create a dictionary to store periodTypes (used when we output the shifts to the shifts csv)
	periodTypes = dict()

	for i, jEv in enumerate(events):

		newDict = dict()

		# Create a dictionary for this event
		newDict["id"] = jEv["about"]["eventIdx"]
		newDict["order"] = i

		newDict["period"] = jEv["about"]["period"]
		newDict["periodType"] = jEv["about"]["periodType"].lower()
		newDict["time"] = toSecs(jEv["about"]["periodTime"])

		newDict["description"] = jEv["result"]["description"]
		newDict["type"] = jEv["result"]["eventTypeId"].lower()
		if "secondaryType" in jEv["result"]:
			newDict["subtype"] = jEv["result"]["secondaryType"].lower()

		# Record penalty-specific information
		if newDict["type"] == "penalty":
			newDict["penSeverity"] = jEv["result"]["penaltySeverity"].lower()
			newDict["penaltyMinutes"] = jEv["result"]["penaltyMinutes"]	

		if "coordinates" in jEv and len(jEv["coordinates"]) == 2:
			newDict["locX"] = jEv["coordinates"]["x"]
			newDict["locY"] = jEv["coordinates"]["y"]

			#
			# If coordinates exist, translate coordinates to zones
			#

			# Determine whether the home team's defensive zone has x < 0 or x > 0
			# Starting in 2014-2015, teams switch ends prior to the start of OT in the regular season
			hDefZoneIsNegX = None
			if newDict["period"] % 2 == 0:	# For even-numbered periods (2, 4, etc.), the home team's def. zone has x > 0
				hDefZoneIsNegX = False
			else:							# For even-numbered periods (1, 3, etc.), the home team's def. zone has x < 0
				hDefZoneIsNegX = True

			# Exceptions
			# For the Winter Classic on Jan 1, 2015, teams switched sides at the 10 minute mark of the first period
			if seasonArg == 20142015 and gameId == 20556:
				if newDict["period"] == 1 and newDict["time"] < 10 * 60:
					hDefZoneIsNegX = True
				elif newDict["period"] == 1 and newDict["time"] >= 10 * 60:
					hDefZoneIsNegX = False

			# Store the event's zone from the home team's perspective
			# Redlines are located at x = -25 and +25
			if newDict["locX"] >= -25 and newDict["locX"] <= 25:
				newDict["hZone"] = "n"
			elif hDefZoneIsNegX == True:
				if newDict["locX"] < -25:
					newDict["hZone"] = "d"
				elif newDict["locX"] > 25:
					newDict["hZone"] = "o"
			elif hDefZoneIsNegX == False:
				if newDict["locX"] < -25:
					newDict["hZone"] = "o"
				elif newDict["locX"] > 25:
					newDict["hZone"] = "d"

		# Record players and their roles
		# Some additional processing required:
		# 	For goals, the json simply lists "assist" for both assisters - enhance this to "assist1" and "assist2"
		#	For giveaways and takeaways, the json uses role "PlayerID" - convert this to "giver" and "taker"
		#	For "puck over glass" penalties, there seems to be a bug:
		#		The json description in 2015020741 is: Braden Holtby Delaying Game - Puck over glass served by Alex Ovechkin
		#		However, Ovechkin is given the playerType: "DrewBy" -- we're going to correct this by giving him type "ServedBy"

		jRoles = dict()
		for jP in jEv["players"]:

			role = jP["playerType"].lower()

			if newDict["type"] == "giveaway":
				role = "giver"
			elif newDict["type"] == "takeaway":
				role = "taker"
			elif newDict["type"] == "goal":
				# Assume that in jEv["players"], the scorer is always listed first, the primary assister listed second, and secondary assister listed third
				if role == "assist" and jP["player"]["id"] == jEv["players"][1]["player"]["id"]:
					role = "assist1"
				elif role == "assist" and jP["player"]["id"] == jEv["players"][2]["player"]["id"]:
					role = "assist2"

			jRoles[role] = jP["player"]["id"]
		
		if newDict["type"] == "penalty":
			if newDict["subtype"].lower().find("puck over glass") >= 0:
				if "servedby" not in jRoles and "drewby" in jRoles:
					jRoles["servedby"] = jRoles["drewby"]
					del jRoles["drewby"]

		# If there's no roles - we don't want to create a 'roles' key in the event's output dictionary
		if len(jRoles) > 0:
			newDict["roles"] = jRoles

		# Record event team and iceSit sfrom json - use the team abbreviation, not home/away
		# For face-offs, the json's event team is the winner
		# For blocked shots, the json's event team is the blocking team - we want to change this to the shooting team
		# For penalties, the json's event team is the team who took the penalty
		if "team" in jEv:
			newDict["team"] = teamAbbrevs[jEv["team"]["name"].lower()]
			if newDict["type"] == "blocked_shot":
				if newDict["team"] == outTeams["home"]["abbrev"]:
					newDict["team"] = outTeams["away"]["abbrev"]
				elif newDict["team"] == outTeams["away"]["abbrev"]:
					newDict["team"] = outTeams["home"]["abbrev"]

			if newDict["team"] == outTeams["home"]["abbrev"]:
				newDict["iceSit"] = "home"
			elif newDict["team"] == outTeams["away"]["abbrev"]:
				newDict["iceSit"] = "away"

		# Record period types
		if newDict["period"] not in periodTypes:
			periodTypes[newDict["period"]] = newDict["periodType"] 

		# Record the home and away scores when the event occurred
		# For goals, the json includes the goal itself in the score situation, but it's more accurate to say that the first goal was scored when it was 0-0
		# Don't do this for shootout goals - the json doesn't increment the home and away scores for these
		if newDict["type"] == "goal" and newDict["periodType"] != "shootout":
			if newDict["team"] == outTeams["away"]["abbrev"]:
				newDict["aScore"] = jEv["about"]["goals"]["away"] - 1
				newDict["hScore"] = jEv["about"]["goals"]["home"]	
			elif newDict["team"] == outTeams["home"]["abbrev"]:
				newDict["aScore"] = jEv["about"]["goals"]["away"]
				newDict["hScore"] = jEv["about"]["goals"]["home"] - 1	
		else:
			newDict["aScore"] = jEv["about"]["goals"]["away"]
			newDict["hScore"] = jEv["about"]["goals"]["home"]

		#
		# Add event to event list
		#

		outEvents.append(copy.deepcopy(newDict))

	#
	# Done looping through json events, so clear the original dictionary
	#

	del events

	#
	#
	# Process shift json
	#
	#

	inFile = file(inDir + shiftJson, "r")
	inString = inFile.read()
	jsonDict = json.loads(inString)
	inFile.close()

	shifts = copy.deepcopy(jsonDict["data"])
	jsonDict.clear()

	nestedShifts = dict()
	maxPeriod = 0
	periodDurs = dict()

	#
	# Nest the raw shift data (which is flat) by player
	#

	for s in shifts:

		# json period values: 1, 2, 3, 4 (regular season OT), 5 (regular season SO)
		# For regular season, period 5 is unreliable:
		#	2015020759 went to SO, but only a single player has a shift with period 5, and the start and end times are 0:00 - this was the only SO goal
		pId = s["playerId"]
		period = s["period"]	
		start = toSecs(s["startTime"])
		end = toSecs(s["endTime"])

		# Ignore SO for regular season games
		if (gameId < 30000 and period <= 4) or gameId >= 30000:

			# If the playerId doesn't already exist, then create a new dictionary for the player and store some player properties
			if pId not in nestedShifts:
				nestedShifts[pId] = dict()
				nestedShifts[pId]["position"] = outPlayers[pId]["position"]
				nestedShifts[pId]["team"] = s["teamAbbrev"].lower()

				if nestedShifts[pId]["team"] == outTeams["home"]["abbrev"]:
					nestedShifts[pId]["iceSit"] = "home"
				elif nestedShifts[pId]["team"] == outTeams["away"]["abbrev"]:
					nestedShifts[pId]["iceSit"] = "away"

			# Record the maxPeriod
			if period > maxPeriod:
				maxPeriod = period

			# Record the period lengths by tracking the maximum shift end time
			if period not in periodDurs:
				periodDurs[period] = 0

			if end > periodDurs[period]:
				periodDurs[period] = end

			# Create a dictionary entry for each period
			# The key is the period number (integer) and 2 lists are stored:
			# 1. In "#Set" (where # is a period), the nth second when a player was the ice is stored
			# 		The list of times is 0-based: a player on the ice from 00:00 to 00:05 will have the following entries in the list: 0, 1, 2, 3, 4
			#		This is used to calculate TOIs (the player was on the ice for 5 seconds)
			# 2. in "#Ranges", the [start, end] time of each shift is stored
			#		For a shift from 00:00 to 00:05, [0,5] will be stored
			#		This is used to find on-ice players for events (any event occurring at 00:00, 00:01,..., 00:05) might be attributed to this player's shift
			if str(period) + "Set" not in nestedShifts[pId]:
				nestedShifts[pId][str(period) + "Set"] = []
				nestedShifts[pId][str(period) + "Ranges"] = []

			nestedShifts[pId][str(period) + "Ranges"].append([start, end])
			nestedShifts[pId][str(period) + "Set"].extend(range(start, end))

	#
	# Some players may not have played for an entire period
	# Create a dictionary entry for these periods so we can loop through all periods for all players without any errors
	#

	for pId in nestedShifts:
		for period in range(1, maxPeriod + 1):
			if str(period) + "Set" not in nestedShifts[pId]:
				nestedShifts[pId][str(period) + "Set"] = []
				nestedShifts[pId][str(period) + "Ranges"] = []

	#
	# Process the shifts, one period at a time
	#

	for period in range(1, maxPeriod + 1):

		#
		# Record the number of goalies and skaters on the ice at each second (the list index represents the number of seconds elapsed)
		# For a 20-second period, each of these lists will have length 20 (indices 0 to 19, which matches how we stored times-on-ice in nestedShifts)
		#

		aGCountPerSec = [0] * periodDurs[period]	# Number of away goalies on the ice at each second
		hGCountPerSec = [0] * periodDurs[period]	# Number of home goalies on the ice at each second
		aSCountPerSec = [0] * periodDurs[period]	# Number of away skaters on the ice at each second
		hSCountPerSec = [0] * periodDurs[period]	# Number of home skaters on the ice at each second

		# Loop through each player's shifts (for the current period)
		# Increment the skater counts for the times when the player was on the ice
		for pId in nestedShifts:
			if nestedShifts[pId]["iceSit"] == "home":
				if nestedShifts[pId]["position"] == "g":
					for sec in nestedShifts[pId][str(period) + "Set"]:
						hGCountPerSec[sec] += 1
				else:
					for sec in nestedShifts[pId][str(period) + "Set"]:
						hSCountPerSec[sec] += 1
			elif nestedShifts[pId]["iceSit"] == "away":
				if nestedShifts[pId]["position"] == "g":
					for sec in nestedShifts[pId][str(period) + "Set"]:
						aGCountPerSec[sec] += 1
				else:
					for sec in nestedShifts[pId][str(period) + "Set"]:
						aSCountPerSec[sec] += 1

		#
		# For each team's strength situations, store the seconds (as a set) at which the situation occurred
		#

		strSitSecs = dict()
		for strSit in strengthSits:
			strSitSecs[strSit] = dict()
			strSitSecs[strSit]["home"] = set()
			strSitSecs[strSit]["away"] = set()

		# Loop through each second of the period, sec
		# Check the number of goalies and skaters on the ice to determine the strength situation at time sec
		# Add sec to the appropriate sets in strSitSecs
		for sec in range(0, periodDurs[period]):
			if aGCountPerSec[sec] == 0:
				strSitSecs["ownGPulled"]["away"].add(sec)
				strSitSecs["oppGPulled"]["home"].add(sec)
			elif hGCountPerSec[sec] == 0:
				strSitSecs["ownGPulled"]["home"].add(sec)
				strSitSecs["oppGPulled"]["away"].add(sec)
			elif aSCountPerSec[sec] > hSCountPerSec[sec] and aSCountPerSec[sec] >= 4 and hSCountPerSec[sec] >= 3:
				# Cases with away PP and home SH (5v4, 5v3, 4v3)
				aKey = "pp" + str(aSCountPerSec[sec]) + str(hSCountPerSec[sec])
				hKey = "sh" + str(hSCountPerSec[sec]) + str(aSCountPerSec[sec])
				strSitSecs[aKey]["away"].add(sec)
				strSitSecs[hKey]["home"].add(sec)
			elif aSCountPerSec[sec] < hSCountPerSec[sec] and aSCountPerSec[sec] >= 3 and hSCountPerSec[sec] >= 4:
				# Cases with home PP and away SH (5v4, 5v3, 4v3)
				aKey = "sh" + str(aSCountPerSec[sec]) + str(hSCountPerSec[sec])
				hKey = "pp" + str(hSCountPerSec[sec]) + str(aSCountPerSec[sec])
				strSitSecs[aKey]["away"].add(sec)
				strSitSecs[hKey]["home"].add(sec)
			elif aSCountPerSec[sec] == hSCountPerSec[sec] and aSCountPerSec[sec]:
				key = "ev" + str(aSCountPerSec[sec])
				strSitSecs[key]["away"].add(sec)
				strSitSecs[key]["home"].add(sec)
			else:
				key = "other"
				strSitSecs[key]["away"].add(sec)
				strSitSecs[key]["home"].add(sec)

		#
		# Record the score differential at each second (the list index represents the number of seconds elapsed)
		# The score differential is calculated from the home team's perspective (home - away)
		#

		periodStart = [ev for ev in outEvents if ev["type"] == "period_start" and ev["period"] == period][0]
		goals = [ev for ev in outEvents if ev["type"] == "goal" and ev["period"] == period]

		# Initialize the dictionary by setting each second to the score differential at the period's start
		scoreDiffPerSec = dict()
		scoreDiffPerSec["home"] = [periodStart["hScore"] - periodStart["aScore"]] * periodDurs[period]
		scoreDiffPerSec["away"] = [periodStart["aScore"] - periodStart["hScore"]] * periodDurs[period]

		# For each goal, update the score situation at the time of the goal until the period's end
		for goal in goals:
			if goal["team"] == outTeams["home"]["abbrev"]:
				for sec in range(goal["time"], periodDurs[period]):
					scoreDiffPerSec["home"][sec] += 1
					scoreDiffPerSec["away"][sec] -= 1
			elif goal["team"] == outTeams["away"]["abbrev"]:
				for sec in range(goal["time"], periodDurs[period]):
					scoreDiffPerSec["away"][sec] += 1
					scoreDiffPerSec["home"][sec] -= 1

		#
		# For each score situation, store the seconds (as a set) at which the situation occurred
		#

		# Initialize dictionary
		scoreSitSecs = dict()
		scoreSitSecs["home"] = dict()
		scoreSitSecs["away"] = dict()
		for iceSit in scoreSitSecs:
			for scoreSit in scoreSits:
				scoreSitSecs[iceSit][scoreSit] = set()

		# Populate the dictionary
		for sec in range(0, periodDurs[period]):

			# Limit score situations to +/- 3
			hAdjScoreSit = max(-3, min(3, scoreDiffPerSec["home"][sec]))
			aAdjScoreSit = max(-3, min(3, scoreDiffPerSec["away"][sec]))

			# Add the current second to the corresponding set of times
			scoreSitSecs["home"][hAdjScoreSit].add(sec)
			scoreSitSecs["away"][aAdjScoreSit].add(sec)

		#
		# Increment player toi for each score and strength situation
		#

		for pId in nestedShifts:

			# Convert each player's list of times when they were on the ice to a set, so that we can use intersections
			nestedShifts[pId][str(period) + "Set"] = set(nestedShifts[pId][str(period) + "Set"])

			iceSit = nestedShifts[pId]["iceSit"]

			# For each score situation, increment tois for each strength situation (increment because we're adding this period's toi to previous periods' tois)
			for scoreSit in scoreSits:
				for strSit in strengthSits:
					outPlayers[pId][strSit][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][str(period) + "Set"], strSitSecs[strSit][iceSit], scoreSitSecs[iceSit][scoreSit]))

		#
		# Increment team toi for each score and strength situation
		#

		for scoreSit in scoreSits:
			for strSit in strengthSits:
				for iceSit in ["away", "home"]:
					outTeams[iceSit][strSit][scoreSit]["toi"] += len(set.intersection(strSitSecs[strSit][iceSit], scoreSitSecs[iceSit][scoreSit]))

	#
	# Done looping through each period and processing shifts
	#

	#
	#
	# Based on the shift data, append on-ice skaters and goalies to the event data
	#
	#
	
	# Create nested dictionaries that use period and time as keys, and a list of events that occurred at that time as the value
	nestedEvents = dict()
	for period in range(1, maxPeriod + 1):
		nestedEvents[period] = dict()

	for ev in outEvents:
		if ev["time"] not in nestedEvents[ev["period"]]:
			nestedEvents[ev["period"]][ev["time"]] = []
		nestedEvents[ev["period"]][ev["time"]].append(ev)

	# Enhance the events with on-ice skaters 
	for period in range(1, maxPeriod + 1):
		for sec in nestedEvents[period]:

			# Get sets of away and home players that were on-ice at second s - use sets to prevent duplicate entries
			hOnIce = set()
			aOnIce = set()
			aOnIceEnding = set()
			hOnIceEnding= set()
			aOnIceStarting = set()
			hOnIceStarting = set()

			for pId in nestedShifts:
				for shift in nestedShifts[pId][str(period) + "Ranges"]:

					# Players on ice at second s
					if shift[0] <= sec and shift[1] >= sec:
						if nestedShifts[pId]["iceSit"] == "away":
							aOnIce.add(pId)
						elif nestedShifts[pId]["iceSit"] == "home":
							hOnIce.add(pId)

					# Players on ice that are ending their shift at second s
					if shift[1] == sec:
						if nestedShifts[pId]["iceSit"] == "away":
							aOnIceEnding.add(pId)
						elif nestedShifts[pId]["iceSit"] == "home":
							hOnIceEnding.add(pId)

					# Players on ice that are starting their shift at second s
					if shift[0] == sec:
						if nestedShifts[pId]["iceSit"] == "away":
							aOnIceStarting.add(pId)
						elif nestedShifts[pId]["iceSit"] == "home":
							hOnIceStarting.add(pId)

			# Convert sets back to lists
			# hOnIce = list(hOnIce)
			# aOnIce = list(aOnIce)
			# aOnIceEnding = list(aOnIceEnding)
			# hOnIceEnding = list(hOnIceEnding)
			# aOnIceStarting = list(aOnIceStarting)
			# hOnIceStarting = list(hOnIceStarting)


			# CASE 1: Single event at second s, and it coincides with shift changes
			#	Attribute the event to onIce - onIceStarting
			#	This works even if it doesn't coincide with shift changes, because onIceStarting will be empty
			# CASE 2: Multiple events at second s, and it coincides with shift changes
			#	For faceoff events, attribute the faceoff to onIce - onIceEnding
			#		Check if there's any cases with multiple faceoffs (check if there's a STOP event that corresponds to improper faceoffs, because the on-ice players for these should be the same as the faceoff players)
			# 	For all other events, attribute the events to onIce - onIceStarting
			#	Case 2 includes faceoffs at the start of the period - other events like period_start and period_official occur at the same time
			# CASE 3: Penalty shots - these have 3 events listed:
			#	1. The penalty - list onIce - onIceStarting players (same as case 2)
			#	2. The penalty shot - list the shooter and goalie (exception to case 2); get this from the json roles
			#	3. The ensuing faceoff - list onIce - onIceEnding players (same as case 2)

			# adjAOnIce = None
			# adjHOnIce = None
			# if len(nestedEvents[period][sec]) == 1:		# CASE 1

			# 	ev = nestedEvents[period][sec][0]

			# 	adjAOnIce = aOnIce - aOnIceStarting
			# 	adjHOnIce = hOnIce - hOnIceStarting

			# 	# Store the list of adjusted on-ice players
			# 	for pId in adjAOnIce:
			# 		if nestedShifts[pId]["position"] == "g":
			# 			ev["aG"] = pId
			# 		else:
			# 			if "aSkaters" not in ev:
			# 				ev["aSkaters"] = []
			# 			ev["aSkaters"].append(pId)
			# 			ev["aSkaterCount"] = len(ev["aSkaters"])

			# 	for pId in adjHOnIce:
			# 		if nestedShifts[pId]["position"] == "g":
			# 			ev["hG"] = pId
			# 		else:
			# 			if "hSkaters" not in ev:
			# 				ev["hSkaters"] = []
			# 			ev["hSkaters"].append(pId)
			# 			ev["hSkaterCount"] = len(ev["hSkaters"])

			# elif len(nestedEvents[period][sec]) > 1:	# CASE 2

			# 	penaltyShotFound = False
			# 	faceoffsFound = 0
			# 	for ev in nestedEvents[period][sec]:
			# 		if ev["type"] == "penalty" and ev["penSeverity"] == "penalty shot":
			# 			penaltyShotFound = True
			# 		if ev["type"] == "faceoff":
			# 			faceoffsFound += 1

			# 	if faceoffsFound > 1:
			# 		print str(faceoffsFound) + " faceoffs found!"

			# 	if penaltyShotFound == False:
			# 		for ev in nestedEvents[period][sec]:
			# 			if ev["type"] == "faceoff":
			# 				adjAOnIce = aOnIce - aOnIceEnding
			# 				adjHOnIce = hOnIce - hOnIceEnding
			# 			else:
			# 				adjAOnIce = aOnIce - aOnIceStarting
			# 				adjHOnIce = hOnIce - hOnIceStarting

			# 			# Store the list of adjusted on-ice players
			# 			for pId in adjAOnIce:
			# 				if nestedShifts[pId]["position"] == "g":
			# 					ev["aG"] = pId
			# 				else:
			# 					if "aSkaters" not in ev:
			# 						ev["aSkaters"] = []
			# 					ev["aSkaters"].append(pId)
			# 					ev["aSkaterCount"] = len(ev["aSkaters"])

			# 			for pId in adjHOnIce:
			# 				if nestedShifts[pId]["position"] == "g":
			# 					ev["hG"] = pId
			# 				else:
			# 					if "hSkaters" not in ev:
			# 						ev["hSkaters"] = []
			# 					ev["hSkaters"].append(pId)
			# 					ev["hSkaterCount"] = len(ev["hSkaters"])

			# 	elif penaltyShotFound == True:
			# 		for ev in nestedEvents[period][sec]:
			# 			if ev["type"] == "faceoff":
			# 				adjAOnIce = aOnIce - aOnIceEnding
			# 				adjHOnIce = hOnIce - hOnIceEnding
			# 			elif ev["type"] == "shot":
			# 				#adjAOnIce = "shooter or goalie"
			# 				#adjHOnIce = "shooter or goalie"
			# 				adjAOnIce = aOnIce - aOnIceStarting
			# 				adjHOnIce = hOnIce - hOnIceStarting
			# 			else: 
			# 				adjAOnIce = aOnIce - aOnIceStarting
			# 				adjHOnIce = hOnIce - hOnIceStarting

			# 			# Store the list of adjusted on-ice players
			# 			for pId in adjAOnIce:
			# 				if nestedShifts[pId]["position"] == "g":
			# 					ev["aG"] = pId
			# 				else:
			# 					if "aSkaters" not in ev:
			# 						ev["aSkaters"] = []
			# 					ev["aSkaters"].append(pId)
			# 					ev["aSkaterCount"] = len(ev["aSkaters"])

			# 			for pId in adjHOnIce:
			# 				if nestedShifts[pId]["position"] == "g":
			# 					ev["hG"] = pId
			# 				else:
			# 					if "hSkaters" not in ev:
			# 						ev["hSkaters"] = []
			# 					ev["hSkaters"].append(pId)
			# 					ev["hSkaterCount"] = len(ev["hSkaters"])

			# ALTERNATIVELY
			# Case 1: Attribute faceoffs to onIce - onIceEnding
			# Case 2: Attribute all other events to onIce - onIceStarting
			for ev in nestedEvents[period][sec]:

				if ev["type"] not in ["period_ready", "period_official", "game_end", "game_scheduled"]:	# Don't append skaters to these events
					adjAOnIce = None
					adjHOnIce = None

					if ev["type"] == "faceoff" or ev["type"] == "period_start":
						adjAOnIce = aOnIce - aOnIceEnding
						adjHOnIce = hOnIce - hOnIceEnding
					else:
						adjAOnIce = aOnIce - aOnIceStarting
						adjHOnIce = hOnIce - hOnIceStarting

					# Store the list of on-ice players
					for pId in adjAOnIce:
						if nestedShifts[pId]["position"] == "g":
							ev["aG"] = pId
						else:
							if "aSkaters" not in ev:
								ev["aSkaters"] = []
							ev["aSkaters"].append(pId)
							ev["aSkaterCount"] = len(ev["aSkaters"])

					for pId in adjHOnIce:
						if nestedShifts[pId]["position"] == "g":
							ev["hG"] = pId
						else:
							if "hSkaters" not in ev:
								ev["hSkaters"] = []
							ev["hSkaters"].append(pId)
							ev["hSkaterCount"] = len(ev["hSkaters"])

					print str(ev["aSkaterCount"]) + " -- " + str(ev["hSkaterCount"])

	pprint(outEvents)
	#
	#
	# For each event, increment player and team stats
	#
	#

	for ev in outEvents:

		# Don't increment stats for events in regular season shoot-outs
		if gameId < 30000 and ev["period"] >= 5:
			continue
		elif ev["type"] in ["goal", "shot", "missed_shot", "blocked_shot", "faceoff", "penalty"]:

			aAbbrev = outTeams["away"]["abbrev"]
			hAbbrev = outTeams["home"]["abbrev"]

			#
			# Get the score situation for each team
			#

			teamScoreSits = dict()	# Returns the score situation from the key-team's perspective
			teamScoreSits[aAbbrev] = max(-3, min(3, ev["aScore"] - ev["hScore"]))
			teamScoreSits[hAbbrev] = max(-3, min(3, ev["hScore"] - ev["aScore"]))

			#
			# Get the strength situation for each team
			#
			
			teamStrengthSits = dict()	# Returns the strength situation from the key-team's perspective

			if "aG" not in ev:
				teamStrengthSits[aAbbrev] = "ownGPulled"
				teamStrengthSits[hAbbrev] = "oppGPulled"
			elif "hG" not in ev:
				teamStrengthSits[aAbbrev] = "oppGPulled"
				teamStrengthSits[hAbbrev] = "ownGPulled"
			elif ev["aSkaterCount"] > ev["hSkaterCount"] and ev["aSkaterCount"] >= 4 and ev["hSkaterCount"] >= 3:
				# Cases with away PP and home SH (5v4, 5v3, 4v3)
				teamStrengthSits[aAbbrev] = "pp" + str(ev["aSkaterCount"]) + str(ev["hSkaterCount"])
				teamStrengthSits[hAbbrev] = "sh" + str(ev["hSkaterCount"]) + str(ev["aSkaterCount"])
			elif ev["aSkaterCount"] < ev["hSkaterCount"] and ev["aSkaterCount"] >= 3 and ev["hSkaterCount"] >= 4:
				# Cases with home PP and away SH (5v4, 5v3, 4v3)
				teamStrengthSits[aAbbrev] = "sh" + str(ev["aSkaterCount"]) + str(ev["hSkaterCount"])
				teamStrengthSits[hAbbrev] = "pp" + str(ev["hSkaterCount"]) + str(ev["aSkaterCount"])
			elif ev["aSkaterCount"] == ev["hSkaterCount"] and ev["aSkaterCount"] >= 3:
				teamStrengthSits[aAbbrev] = "ev" + str(ev["aSkaterCount"])
				teamStrengthSits[hAbbrev] = "ev" + str(ev["hSkaterCount"])
			else:
				teamStrengthSits[aAbbrev] = "other"
				teamStrengthSits[hAbbrev] = "other"

			#
			# Increment individual stats
			#

			# Get the event team and opposing team, since some events affect an opponent's stats (e.g, blocked shots)
			evTeam = ev["team"]
			oppTeam = None
			if evTeam == aAbbrev:
				oppTeam = hAbbrev
			elif evTeam == hAbbrev:
				oppTeam = aAbbrev

			if ev["type"] == "goal":
				outPlayers[ev["roles"]["scorer"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ig"] += 1
				outPlayers[ev["roles"]["scorer"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
				if "assist1" in ev["roles"]:
					outPlayers[ev["roles"]["assist1"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ia1"] += 1
				if "assist2" in ev["roles"]:
					outPlayers[ev["roles"]["assist2"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ia2"] += 1
			elif ev["type"] == "shot":
				# A "goalie" role also exists for saved shots, but we ignore this
				outPlayers[ev["roles"]["shooter"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
			elif ev["type"] == "missed_shot":
				outPlayers[ev["roles"]["shooter"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ims"] += 1
			elif ev["type"] == "blocked_shot":
				outPlayers[ev["roles"]["shooter"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ibs"] += 1
				outPlayers[ev["roles"]["blocker"]][teamStrengthSits[oppTeam]][teamScoreSits[oppTeam]]["blocked"] += 1
			elif ev["type"] == "penalty":
				if "drewby" in ev["roles"]:
					outPlayers[ev["roles"]["drewby"]][teamStrengthSits[oppTeam]][teamScoreSits[oppTeam]]["penDrawn"] += 1
				if "penaltyon" in ev["roles"]:
					outPlayers[ev["roles"]["penaltyon"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["penTaken"] += 1
			elif ev["type"] == "faceoff":
				outPlayers[ev["roles"]["winner"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["foWon"] += 1
				outPlayers[ev["roles"]["loser"]][teamStrengthSits[oppTeam]][teamScoreSits[oppTeam]]["foLost"] += 1

			#
			# Increment stats for on-ice players
			#

			# List all on-ice HOME players
			hPlayers = []
			if "hSkaters" in ev:
				hPlayers.extend(ev["hSkaters"])
			if "hG" in ev:
				hPlayers.append(ev["hG"])

			for pId in hPlayers:
				if ev["type"] == "goal":
					if evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["gf"] += 1
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["sf"] += 1
					elif evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["ga"] += 1
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["sa"] += 1
				elif ev["type"] == "shot":
					if evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["sf"] += 1
					elif evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["sa"] += 1
				elif ev["type"] == "missed_shot":
					if evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["msf"] += 1
					elif evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["msa"] += 1
				elif ev["type"] == "blocked_shot":
					if evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["bsf"] += 1
					elif evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["bsa"] += 1
				elif ev["type"] == "faceoff":
					# For face-off zone counts, we don't care who won (the evTeam) - we're just tracking how many o/d/n FOs the player was on the ice for
					zonePrefix = ev["hZone"]
					outPlayers[pId][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]][zonePrefix + "fo"] += 1
			
			# List all on-ice AWAY players
			aPlayers = []
			if "aSkaters" in ev:
				aPlayers.extend(ev["aSkaters"])
			if "aG" in ev:
				aPlayers.append(ev["aG"])

			for pId in aPlayers:
				if ev["type"] == "goal":
					if evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["gf"] += 1
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["sf"] += 1
					elif evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["ga"] += 1
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["sa"] += 1
				elif ev["type"] == "shot":
					if evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["sf"] += 1
					elif evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["sa"] += 1
				elif ev["type"] == "missed_shot":
					if evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["msf"] += 1
					elif evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["msa"] += 1
				elif ev["type"] == "blocked_shot":
					if evTeam == aAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["bsf"] += 1
					elif evTeam == hAbbrev:
						outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["bsa"] += 1
				elif ev["type"] == "faceoff":
					# For face-off zone counts, we don't care who won (the evTeam) - we're just tracking how many o/d/n FOs the player was on the ice for
					# Since outEvents[ev]["hZone"] is always from the home-team's perspective, we need to flip the o-zone and d-zone for the away-team
					zonePrefix = None
					if ev["hZone"] == "o":
						zonePrefix = "d"
					elif ev["hZone"] == "d":
						zonePrefix = "o"
					elif ev["hZone"] == "n":
						zonePrefix = "n"
					outPlayers[pId][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]][zonePrefix + "fo"] += 1

			#
			# Increment stats for teams
			#

			hSuffix = None
			aSuffix = None
			if evTeam == hAbbrev:
				hSuffix = "f"
				aSuffix = "a"
			elif evTeam == aAbbrev:
				hSuffix = "a"
				aSuffix = "f"

			if ev["type"] == "goal":
				outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["g" + hSuffix] += 1
				outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["s" + hSuffix] += 1
				outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["g" + aSuffix] += 1
				outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["s" + aSuffix] += 1
			elif ev["type"] == "shot":
				outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["s" + hSuffix] += 1
				outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["s" + aSuffix] += 1
			elif ev["type"] == "missed_shot":
				outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["ms" + hSuffix] += 1
				outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["ms" + aSuffix] += 1
			elif ev["type"] == "blocked_shot":
				outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["bs" + hSuffix] += 1
				outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["bs" + aSuffix] += 1
			elif ev["type"] == "penalty":
				if evTeam == hAbbrev:
					outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["penTaken"] += 1
					outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["penDrawn"] += 1
				elif evTeam == hAbbrev:
					outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["penTaken"] += 1
					outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["penDrawn"] += 1
			elif ev["type"] == "faceoff":
				# Increment o/d/n faceoffs for the home team
				evHZone = ev["hZone"]
				outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]][evHZone + "fo"] += 1

				# Increment o/d/n faceoffs for the away team
				evAZone = None
				if evHZone == "o":
					evAZone = "d"
				elif evHZone == "d":
					evAZone = "o"
				elif evHZone == "n":
					evAZone = "n"
				outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]][evAZone + "fo"] += 1

				# Increment foWon/foLost counts
				if evTeam == hAbbrev:
					outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["foWon"] += 1
					outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["foLost"] += 1
				elif evTeam == aAbbrev:
					outTeams["away"][teamStrengthSits[aAbbrev]][teamScoreSits[aAbbrev]]["foWon"] += 1
					outTeams["home"][teamStrengthSits[hAbbrev]][teamScoreSits[hAbbrev]]["foLost"] += 1
	
	#
	# Done incrementing player and team stats
	#

	#
	#
	# Prepare output files that will be loaded into the database
	# Use .encode("utf-8") when writing the output string to handle accents in player names and French descriptions
	#
	#

	print "- - - - -"
	print "Preparing csv files"

	#
	# Output shifts
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-shifts.csv", "w")
	outString = "season,date,gameId,team,iceSit,playerId,position,period,periodType,start,end\n"
	outFile.write(outString)

	for sh in shifts:
		outString = str(seasonArg)
		outString += "," + str(gameDate)
		outString += "," + str(gameId)
		outString += "," + nestedShifts[sh["playerId"]]["team"]
		outString += "," + nestedShifts[sh["playerId"]]["iceSit"]
		outString += "," + str(sh["playerId"])
		outString += "," + nestedShifts[pId]["position"]
		outString += "," + str(sh["period"])
		outString += "," + periodTypes[sh["period"]]
		outString += "," + str(toSecs(sh["startTime"]))
		outString += "," + str(toSecs(sh["endTime"]))
		outString += "\n"
		outFile.write(outString.encode("utf-8"))

	outFile.close()

	#
	# Output events
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-events.csv", "w")
	outString = "season,date,gameId,eventId,"
	outString += "period,periodType,time,aScore,hScore,aSkaters,hSkaters,hZone,locX,locY,"
	outString += "desc,type,subtype,"
	outString += "team,teamIceSit,"	# team is the event team; teamIceSit is home/away for the event team
	outString += "p1,p2,p3,p1Role,p2Role,p3Role,"
	outString += "aS1,aS2,aS3,aS4,aS5,aS6,aG,"
	outString += "hS1,hS2,hS3,hS4,hS5,hS6,hG\n"
	outFile.write(outString)

	for ev in outEvents:

		outString = str(seasonArg)
		outString += "," + str(gameDate)
		outString += "," + str(gameId)
		outString += "," + str(ev["id"])

		outString += "," + str(ev["period"])
		outString += "," + str(ev["periodType"])
		outString += "," + str(ev["time"])
		outString += "," + str(ev["aScore"])
		outString += "," + str(ev["hScore"])
		outString += "," + outputVal(ev, "aSkaterCount")
		outString += "," + outputVal(ev, "hSkaterCount")
		outString += "," + outputVal(ev, "hZone")
		outString += "," + outputVal(ev, "locX")
		outString += "," + outputVal(ev, "locY")

		outString += "," + ev["description"].replace(",", ";") # Replace commas to maintain the csv structure
		outString += "," + ev["type"]
		outString += "," + outputVal(ev, "subtype")

		outString += "," + outputVal(ev, "team")
		outString += "," + outputVal(ev, "iceSit")

		#
		# Process roles
		#

		if "roles" not in ev:
			outString += ",NULL,NULL,NULL,NULL,NULL,NULL"
		else:
			pIdString = ""
			roleString = ""
			# Append playerIds and roles
			roleCount = 0
			for role in ev["roles"]:
				pIdString += "," + str(ev["roles"][role])
				roleString += "," + role
				roleCount += 1
			# If there are less than 3 playerIds, pad the shortage with NULL values
			while roleCount < 3:
				pIdString += ",NULL"
				roleString += ",NULL"
				roleCount += 1
			# Add the playerIds and roles to the output
			outString += pIdString + roleString

		#
		# Append on-ice playerIds and goalieIds
		# Start with the AWAY team
		#

		for prefix in ["a", "h"]:

			# SKATERS
			pIdString = ""
			if (prefix + "Skaters") not in ev:
				outString += ",NULL,NULL,NULL,NULL,NULL,NULL"
			else:
				# Append playerIds
				count = 0
				for pId in ev[prefix + "Skaters"]:
					pIdString += "," + str(pId)
					count += 1
				# If there are less than 6 skater playerIds, pad the shortage with NULLs
				while count < 6:
					pIdString += ",NULL"
					count += 1
			outString += pIdString

			# GOALIE
			outString += "," + outputVal(ev, prefix + "G")

		# Write event to output file
		outString += "\n"
		outFile.write(outString.encode("utf-8"))

	outFile.close()

	#
	# Output team stats
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-teams.csv", "w")
	outString = "season,date,gameId,team,iceSit,strengthSit,scoreSit"
	for stat in teamStats:
		outString += "," + stat
	outString += "\n"
	outFile.write(outString)

	for iceSit in outTeams:
		for strSit in strengthSits:
			for scSit in outTeams[iceSit][strSit]:
				outString = str(seasonArg)
				outString += "," + str(gameDate)
				outString += "," + str(gameId)
				outString += "," + outTeams[iceSit]["abbrev"]
				outString += "," + iceSit
				outString += "," + strSit
				outString += "," + str(scSit)

				# Append each stat to the output string
				# If all stats are equal to 0, then don't output the record
				allZero = True
				for stat in teamStats:
					outString += "," + str(outTeams[iceSit][strSit][scSit][stat])
					if outTeams[iceSit][strSit][scSit][stat] != 0:
						allZero = False
				outString += "\n"

				if allZero == False:
					outFile.write(outString.encode("utf-8"))
					
	outFile.close()

	#
	# Output player stats
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-players.csv", "w")
	outString = "season,date,gameId,team,iceSit,playerId,position,strengthSit,scoreSit"
	for stat in playerStats:
		outString += "," + stat
	outString += "\n"
	outFile.write(outString)

	for pId in outPlayers:
		for strSit in strengthSits:
			for scSit in outPlayers[pId][strSit]:
				outString = str(seasonArg)
				outString += "," + str(gameDate)
				outString += "," + str(gameId)
				outString += "," + outPlayers[pId]["team"]
				outString += "," + outPlayers[pId]["iceSit"]
				outString += "," + str(pId)
				outString += "," + outPlayers[pId]["position"]
				outString += "," + strSit
				outString += "," + str(scSit)

				# Append each stat to the output string
				# If all stats are equal to 0, then don't output the record
				allZero = True
				for stat in playerStats:
					outString += "," + str(outPlayers[pId][strSit][scSit][stat])
					if outPlayers[pId][strSit][scSit][stat] != 0:
						allZero = False
				outString += "\n"

				if allZero == False:
					outFile.write(outString.encode("utf-8"))

	outFile.close()

	#
	# Output rosters
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-rosters.csv", "w")
	outString = "season,date,gameId,team,iceSit,playerId,firstName,lastName,jersey,position\n"
	outFile.write(outString)

	for pId in outPlayers:

		outString = str(seasonArg)
		outString += "," + str(gameDate)
		outString += "," + str(gameId)

		outString += "," + outTeams[outPlayers[pId]["iceSit"]]["abbrev"]
		outString += "," + outPlayers[pId]["iceSit"]

		outString += "," + str(pId)
		outString += "," + outPlayers[pId]["firstName"]
		outString += "," + outPlayers[pId]["lastName"]
		outString += "," + outPlayers[pId]["jersey"]
		outString += "," + outPlayers[pId]["position"]

		outString += "\n"
		outFile.write(outString.encode("utf-8"))

	outFile.close()

	#
	#
	# Load csv files into database
	#
	#
	sys.exit()
	print "- - - - -"
	print "Loading csv files into database"

	# Connect to database
	databaseUser = dbconfig.user
	databasePasswd = dbconfig.passwd
	databaseHost = dbconfig.host
	database = dbconfig.database
	connection = mysql.connector.connect(user=databaseUser, passwd=databasePasswd, host=databaseHost, database=database)
	cursor = connection.cursor()

	# Load csv files into database
	# Use a dictionary to link csv file names (key) with table names (value)
	filesToLoad = dict()
	filesToLoad["-events.csv"] = "game_events"
	filesToLoad["-players.csv"] = "game_player_stats"
	filesToLoad["-teams.csv"] = "game_team_stats"
	filesToLoad["-shifts.csv"] = "game_shifts"
	filesToLoad["-rosters.csv"] = "game_rosters"

	for fileToLoad in filesToLoad:
		fname = outDir + str(seasonArg) + "-" + str(gameId) + fileToLoad
		query = ("LOAD DATA LOCAL INFILE '" + fname + "'"
			+ " REPLACE INTO TABLE " + filesToLoad[fileToLoad]
			+ " FIELDS TERMINATED BY ',' ENCLOSED BY '\"'"
			+ " LINES TERMINATED BY '\\n'"
			+ " IGNORE 1 LINES")
		print "Loading " + fname + " into database"
		cursor.execute(query)

	#
	# Insert game result into database using a prepared statement
	#

	cursor = connection.cursor(prepared=True) # Enable support for prepared statements

	try:
		timeRemaining = linescore["currentPeriodTimeRemaining"].lower()
	except:
		timeRemaining = linescore["currentPeriodTimeRemaining"]

	query = ("REPLACE INTO game_result (season, date, gameId, aTeam, hTeam, aFinal, hFinal, lastPeriodNumber, lastPeriodName, lastPeriodTimeRemaining)"
		+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	args = (seasonArg, gameDate, gameId, outTeams["away"]["abbrev"], outTeams["home"]["abbrev"], linescore["teams"]["away"]["goals"], linescore["teams"]["home"]["goals"], linescore["currentPeriod"], linescore["currentPeriodOrdinal"].lower(), timeRemaining,)
	cursor.execute(query, args)

	# Close connection
	cursor.close()
	connection.close()

	print "Done processing game " + str(gameId)
	print "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"

#
# Done looping through each gameId
#