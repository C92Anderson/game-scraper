# For scraping and processing raw data, and creating csv
import sys
import urllib
import os.path
import json
import copy
import re
from pprint import pprint
from bs4 import BeautifulSoup

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

# The json team abbreviations are different from the html ones:
# N.J (html) -> NJD (json); S.J -> SJS; T.B -> TBL; L.A -> LAK
def useNewTeamAbbrev(abbrev):
	if abbrev == "n.j":
		return "njd"
	elif abbrev == "s.j":
		return "sjs"
	elif abbrev == "t.b":
		return "tbl"
	elif abbrev == "l.a":
		return "lak"
	else:
		return abbrev

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

# Converts event types found in the html file --> event types found in the json file
evTypes = dict()
evTypes["fac"] = "faceoff"
evTypes["shot"] = "shot"
evTypes["miss"] = "missed_shot"
evTypes["block"] = "blocked_shot"
evTypes["penl"] = "penalty"
evTypes["goal"] = "goal"
evTypes["give"] = "giveaway"
evTypes["take"] = "takeaway"
evTypes["hit"] = "hit"
evTypes["stop"] = "stop"
evTypes["pstr"] = "period_start"
evTypes["pend"] = "period_end"
evTypes["gend"] = "game_end"
evTypes["soc"] = "shootout_complete"
evTypes["goff"] = "game_official"	# "goff" isn't included in every html pbp file	
evTypes["chl"] = ""					# league challenge - these don't look like they're captured in the json

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

	# Data we need to record
	gameDate = 0
	players = dict()
	teams = dict()
	events = dict()

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

	scoreSits = [-3, -2, -1, 0, 1, 2, 3]
	strengthSits = ["ownGPulled", "oppGPulled", "sh45", "sh35", "sh34", "pp54", "pp53", "pp43", "ev5", "ev4", "ev3", "other"]
	
	teamIceSits = dict()	# translates the team abbreviation to 'home' or 'away'
	outTeams = dict()		# dictionary to store team information for output

	for iceSit in teams:	# iceSit = 'home' or 'away'

		outTeams[iceSit] = dict()
		outTeams[iceSit]["abbrev"] = teams[iceSit]["abbreviation"].lower()	# team name abbreviation
		outTeams[iceSit]["playerIds"] = []									# list of playerIds

		teamIceSits[outTeams[iceSit]["abbrev"]] = iceSit

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

	outPlayers = dict()
	for pId in players:
		outPlayers[pId] = dict()
		outPlayers[pId]["position"] = players[pId]["primaryPosition"]["abbreviation"].lower()
		outPlayers[pId]["firstName"] = players[pId]["firstName"]
		outPlayers[pId]["lastName"] = players[pId]["lastName"]

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

	# Prepare list of events for output
	outEvents = []

	# Create a dictionary to store periodTypes (used when we output the shifts to the shifts csv)
	periodTypes = dict()

	for jEv in events:

		newDict = dict()

		# Create a dictionary for this event
		newDict["id"] = jEv["about"]["eventIdx"]

		newDict["period"] = jEv["about"]["period"]
		newDict["periodType"] = jEv["about"]["periodType"].lower()
		newDict["time"] = toSecs(jEv["about"]["periodTime"])

		newDict["description"] = jEv["result"]["description"]
		newDict["type"] = jEv["result"]["eventTypeId"].lower()
		if "secondaryType" in jEv["result"]:
			newDict["subtype"] = jEv["result"]["secondaryType"].lower()

		if "coordinates" in jEv and len(jEv["coordinates"]) == 2:
			newDict["locX"] = jEv["coordinates"]["x"]
			newDict["locY"] = jEv["coordinates"]["y"]

		# Record players and their roles
		# Some additional processing required:
		# 	For goals, the json simply lists "assist" for both assisters. Enhance this to "assist1" and "assist2" to match the html roles we created above
		#	For giveaways and takeaways, the json uses role "PlayerID". Convert this to "giver" and "taker" to match the html roles we created above
		#	For saved shots, the json lists the goalie with role "Goalie". The HTML pbp doesn't include the goalie in the event description, so remove this role from the json
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
		
		if newDict["type"] == "shot":
			del jRoles["goalie"]
		elif newDict["type"] == "penalty":
			if newDict["subtype"].lower().find("puck over glass") >= 0:
				if "servedby" not in jRoles and "drewby" in jRoles:
					jRoles["servedby"] = jRoles["drewby"]
					del jRoles["drewby"]

		# If there's no roles, we don't want to create a 'roles' key in the event's output dictionary
		if len(jRoles) > 0:
			newDict["roles"] = jRoles

		#
		# Done getting player roles
		#

		# Record event team from json - use the team abbreviation, not home/away
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

		# Prepare lists to store on-ice players
		newDict["aSkaters"] = []
		newDict["hSkaters"] = []
		newDict["aGoalie"] = None
		newDict["hGoalie"] = None

		#
		# Add event to list
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
			# The key is the period number (integer) and the value is a list of seconds when the player was on the ice
			# The list of times is 0-based: a player on the ice from 00:00 to 00:05 will have the following entries in the list: 0, 1, 2, 3, 4
			if period not in nestedShifts[pId]:
				nestedShifts[pId][period] = []

			nestedShifts[pId][period].extend(range(start, end))

	#
	# Some players may not have played for an entire period
	# Create a dictionary entry for these periods so we can loop through all periods for all players without any errors
	#

	for pId in nestedShifts:
		for period in range(1, maxPeriod + 1):
			if period not in nestedShifts[pId]:
				nestedShifts[pId][period] = []

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
					for sec in nestedShifts[pId][period]:
						hGCountPerSec[sec] += 1
				else:
					for sec in nestedShifts[pId][period]:
						hSCountPerSec[sec] += 1
			elif nestedShifts[pId]["iceSit"] == "away":
				if nestedShifts[pId]["position"] == "g":
					for sec in nestedShifts[pId][period]:
						aGCountPerSec[sec] += 1
				else:
					for sec in nestedShifts[pId][period]:
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
			elif aSCountPerSec[sec] > hSCountPerSec[sec]:	# Cases with home PP and away SH (incl. 5v4, 5v3, 4v3)
				aKey = "pp" + str(aSCountPerSec[sec]) + str(hSCountPerSec[sec])
				hKey = "sh" + str(hSCountPerSec[sec]) + str(aSCountPerSec[sec])
				strSitSecs[aKey]["away"].add(sec)
				strSitSecs[hKey]["home"].add(sec)
			elif aSCountPerSec[sec] < hSCountPerSec[sec]:	# Cases with away PP and home SH (incl. 5v4, 5v3, 4v3)
				aKey = "sh" + str(aSCountPerSec[sec]) + str(hSCountPerSec[sec])
				hKey = "pp" + str(hSCountPerSec[sec]) + str(aSCountPerSec[sec])
				strSitSecs[aKey]["away"].add(sec)
				strSitSecs[hKey]["home"].add(sec)
			elif aSCountPerSec[sec] == hSCountPerSec[sec]:
				key = "ev" + str(aSCountPerSec[sec])
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
			nestedShifts[pId][period] = set(nestedShifts[pId][period])

			iceSit = nestedShifts[pId]["iceSit"]

			# For each score situation, increment tois for each strength situation (increment because we're adding this period's toi to previous periods' tois)
			for scoreSit in scoreSits:
				for strSit in strengthSits:
					outPlayers[pId][strSit][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], strSitSecs[strSit][iceSit], scoreSitSecs[iceSit][scoreSit]))

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
	# Append on-ice skaters and goalies to the event data
	#
	#
	
	for playerId in nestedShifts:					# Loop through each player
		for period in range(1, maxPeriod + 1):		# Loop through each of the player's periods; can't use 'for period in nestedShifts["player"] because there's other keys (team, iceSit, position)
			for ev in outEvents:										# Loop through all events to find events for which the player was on the ice
				if ev["period"] == period:								# We only care about events in the same period as the shifts we're currently looking at
					if ev["time"] in nestedShifts[playerId][period]:	# Check if the event second is in the set of seconds that the place was on the ice
						if nestedShifts[playerId]["position"] == "g":	# Store on-ice goalie
							if nestedShifts[playerId]["iceSit"] == "home":
								ev["hGoalie"] = playerId
							elif nestedShifts[playerId]["iceSit"] == "away":
								ev["aGoalie"] = playerId
						else:											# Store on-ice skater
							if nestedShifts[playerId]["iceSit"] == "home":
								ev["hSkaters"].append(playerId)
							elif nestedShifts[playerId]["iceSit"] == "away":
								ev["aSkaters"].append(playerId)


#
# Done looping through each gameId
#