import sys
import urllib
import os.path
import json
import copy
import re
from pprint import pprint
from bs4 import BeautifulSoup



# Take string "mm:ss" and return the number of seconds (as an integer)
def toSecs(timeStr):
	mm = int(timeStr[0:timeStr.find(":")])
	ss = int(timeStr[timeStr.find(":")+1:])
	return 60 * mm + ss

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

for gameId in gameIds:

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
	pbpHtmlUrl = "http://www.nhl.com/scores/htmlreports/" + str(seasonArg) + "/PL0" + str(gameId) + ".HTM"

	# Downloaded input file names
	shiftJson = str(seasonArg) + "-" + str(gameId) + "-shifts.json"
	pbpJson = str(seasonArg) + "-" + str(gameId) + "-events.json"
	pbpHtml = str(seasonArg) + "-" + str(gameId) + "-events.html"

	# Download files that don't already exist
	filenames = [shiftJson, pbpJson, pbpHtml]
	fileUrls = [shiftJsonUrl, pbpJsonUrl, pbpHtmlUrl]
	for i, filename in enumerate(filenames):
		if os.path.isfile(inDir + filename) == False:
			print "Downloading " + str(filename)
			urllib.urlretrieve(fileUrls[i], inDir + filename)
		else:
			print str(filename) + " already exists"

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
	gameDate = int(gameDate[0:10].replace("-", ""))		# Convert from dateTime format to an int (of the date)
	players = jsonDict["gameData"]["players"]			# Keys: 'ID#' where # is a playerId
	teams = jsonDict["gameData"]["teams"]				# Keys: 'home', 'away'
	events = jsonDict["liveData"]["plays"]["allPlays"]

	# Reformat the keys in the 'players' dictionary: from 'ID#' to # (as an int), where # is the playerId
	tempPlayers = dict()
	for pId in players:
		newKey = int(pId[2:])
		tempPlayers[newKey] = players[pId]
	players = copy.deepcopy(tempPlayers)
	tempPlayers.clear()

	# Create a 'playerIds' dictionary that translates team+jersey (used in the pbp html file) to playerIds
	# Keys: 'home-##' and 'away-##' where ## are jersey numbers
	# Values: playerIds
	playerIds = dict()
	rosters = jsonDict["liveData"]["boxscore"]["teams"]
	aPlayerIds = []
	hPlayerIds = []
	for team in rosters:							# 'team' will be 'home' or 'away'
		for player in rosters[team]["players"]:		# 'player' will be 'ID#' where # is a playerId
			key = team + "-" + rosters[team]["players"][player]["jerseyNumber"]
			playerIds[key] = int(rosters[team]["players"][player]["person"]["id"])

			if team == "home":
				hPlayerIds.append(int(rosters[team]["players"][player]["person"]["id"]))
			elif team == "away":
				aPlayerIds.append(int(rosters[team]["players"][player]["person"]["id"]))

	#
	#
	# Prepare team output
	#
	#

	scoreSits = [-3, -2, -1, 0, 1, 2, 3]
	strengthSits = ["ownGOff", "oppGOff", "pk", "pp", "ev5", "ev4", "ev3"]
	
	teamIceSits = dict()	# translates the team abbreviation to 'home' or 'away'

	outTeams = dict()
	for iceSit in teams:	# iceSit = 'home' or 'away'
		outTeams[iceSit] = dict()
		outTeams[iceSit]["abbrev"] = teams[iceSit]["abbreviation"].lower()

		if iceSit == "home":
			outTeams[iceSit]["playerIds"] = hPlayerIds
		elif iceSit == "away":
			outTeams[iceSit]["playerIds"] = aPlayerIds

		teamIceSits[outTeams[iceSit]["abbrev"]] = iceSit

	#
	#
	# Prepare players output
	#
	#

	outPlayers = dict()
	for pId in players:
		outPlayers[pId] = dict()
		outPlayers[pId]["position"] = players[pId]["primaryPosition"]["abbreviation"].lower()

		# Initialize stats
		for strSit in strengthSits:
			outPlayers[pId][strSit] = dict()

			for scSit in scoreSits:
				outPlayers[pId][strSit][scSit] = dict()

				outPlayers[pId][strSit][scSit]["ig"] = 0		# individual goals
				outPlayers[pId][strSit][scSit]["is"] = 0
				outPlayers[pId][strSit][scSit]["ibs"] = 0
				outPlayers[pId][strSit][scSit]["ims"] = 0
				outPlayers[pId][strSit][scSit]["ia1"] = 0		# primary assists
				outPlayers[pId][strSit][scSit]["ia2"] = 0		# secondary assists
				outPlayers[pId][strSit][scSit]["blocked"] = 0	# shots that this player blocked

				outPlayers[pId][strSit][scSit]["gf"] = 0
				outPlayers[pId][strSit][scSit]["ga"] = 0
				outPlayers[pId][strSit][scSit]["sf"] = 0
				outPlayers[pId][strSit][scSit]["sa"] = 0
				outPlayers[pId][strSit][scSit]["bsf"] = 0
				outPlayers[pId][strSit][scSit]["bsa"] = 0
				outPlayers[pId][strSit][scSit]["msf"] = 0
				outPlayers[pId][strSit][scSit]["msa"] = 0

				outPlayers[pId][strSit][scSit]["ofo"] = 0		# offensive zone face-off
				outPlayers[pId][strSit][scSit]["dfo"] = 0
				outPlayers[pId][strSit][scSit]["nfo"] = 0
				outPlayers[pId][strSit][scSit]["foWon"] = 0		# individual won face-offs
				outPlayers[pId][strSit][scSit]["foLost"] = 0	# individual lost face-offs

				outPlayers[pId][strSit][scSit]["penTaken"] = 0
				outPlayers[pId][strSit][scSit]["penDrawn"] = 0

	#
	#
	# Parse pbpHtml
	#
	#

	inFile = file(inDir + pbpHtml, "r")
	soup = BeautifulSoup(inFile.read(), "lxml")
	inFile.close()

	# Store html events in a dictionary
	# This dictionary will contain all the necessary information to:
	# 1. Match the html events with the json events: period, time, event type, event subtype, p1, p2, p3
	# 2. Enhance the json events with on-ice player information: skater counts, on-ice skater playerIds, on-ice goalie playerIds
	htmlEvents = dict()
	rows = soup.find_all("tr", class_="evenColor")
	for r in rows:

		# Get the event id used in the html file and use it as the key for the dictionary of html events
		htmlId = int(r.find_all("td", class_=re.compile("bborder"))[0].text)
		htmlEvents[htmlId] = dict()

		# Periods in the NHL play-by-play data are always numbered 1, 2, 3, 4, 5 (in regular season, period 5 is the SO)
		# Period numbering in the shift data is different
		htmlEvents[htmlId]["period"] = int(r.find_all("td", class_=re.compile("bborder"))[1].text)

		# Convert elapsed time to seconds
		timeRange = r.find_all("td", class_=re.compile("bborder"))[3]
		timeElapsed = timeRange.find("br").previousSibling
		htmlEvents[htmlId]["time"] = toSecs(timeElapsed)

		# Record the event type
		evType = r.find_all("td", class_=re.compile("bborder"))[4].text.lower()
		htmlEvents[htmlId]["type"] = evType

		# Get the event description
		evDesc = (r.find_all("td", class_=re.compile("bborder"))[5].text).replace(",", ";")
		evDesc = evDesc.replace(unichr(160), " ") # Replace non-breaking spaces with spaces
		htmlEvents[htmlId]["desc"] = evDesc

		# Get the team that TOOK the shot, MADE the hit, or WON the faceoff, etc.
		evTeam = evDesc[0:evDesc.find(" ")].lower()
		if evTeam not in [outTeams["away"]["abbrev"], outTeams["home"]["abbrev"]]:
			evTeam = None
		htmlEvents[htmlId]["team"] = evTeam

		# Get the player jerseys listed in the description
		evPlayerJerseys = []
		numPlayers = evDesc.count("#")
		for i in range(0, numPlayers):
			player = evDesc.split("#")[i + 1]
			player = int(player[0:player.find(" ")])
			evPlayerJerseys.append(player)

		# For face-offs, the pbp html file always lists the away player first, home player second
		# But we want the winner to be eventP1 and the loser to be eventP2, so switch eventP1 and eventP2 if the homeTeam won the faceoff
		if evType == "fac" and evTeam == outTeams["home"]["abbrev"]:
			tempP = evPlayerJerseys[0]
			evPlayerJerseys[0] = evPlayerJerseys[1]
			evPlayerJerseys[1] = tempP

		#
		# Convert jersey numbers into playerIds - depending on the event type, we need to look up the jersey number in the home/away player dictionaries
		#

		evPlayerIds = [-1] * len(evPlayerJerseys)

		if numPlayers >= 1:	 # Cases where 1 jersey listed - here, the listed player usually belongs to the eventTeam

			if evType == "penl" and evDesc.lower().find("player leaves bench") >= 0:
				# EXCEPTION:
				# "S.J TEAM Player leaves bench - bench(2 min), Off. Zone Drawn By: ANA #47 LINDHOLM" - no SJ player is listed
				# See event #341 here: http://www.nhl.com/scores/htmlreports/20142015/PL020120.HTM
				if evTeam == outTeams["away"]["abbrev"]:
					evPlayerIds[0] = playerIds["home-" + str(evPlayerJerseys[0])]
				elif evTeam == outTeams["home"]["abbrev"]:
					evPlayerIds[0] = playerIds["away-" + str(evPlayerJerseys[0])]
			else:
				# This includes penalties where the same player committed and served a "too many men" penalty:
				# "PHI TEAM Too many men/ice - bench(2 min) Served By: #89 GAGNER, Neu. Zone"
				# See event #98 here: http://www.nhl.com/scores/htmlreports/20152016/PL020741.HTM
				if evTeam == outTeams["away"]["abbrev"]:
					evPlayerIds[0] = playerIds["away-" + str(evPlayerJerseys[0])]
				elif evTeam == outTeams["home"]["abbrev"]:
					evPlayerIds[0] = playerIds["home-" + str(evPlayerJerseys[0])]

		if numPlayers >= 2:	# Cases where 2 jerseys are listed

			if (evType in ["fac", "hit", "block"]) or (evType == "penl" and evDesc.lower().find(" served by: ") < 0) or (evType == "penl" and evDesc.lower().find("too many men/ice") >= 0):
			# For these events, eventP2 is eventP1's opponent, so we use the opposite player dictionary.
				if evTeam == outTeams["away"]["abbrev"]:
					evPlayerIds[1] = playerIds["home-" + str(evPlayerJerseys[1])]
				elif evTeam == outTeams["home"]["abbrev"]:
					evPlayerIds[1] = playerIds["away-" + str(evPlayerJerseys[1])]
			elif evType == "goal" or (evType == "penl" and evDesc.lower().find(" served by: ") >= 0): 
			# EXCEPTION:
			# Don't use the opposite dictionary if the penalty description contains "served by" - in this case, the player in the box is on the same team as eventP1. 
			# This case also includes "too many men" bench penalties because P1 is the serving player, P2 is the drawing player: "COL TEAM Too many men/ice - bench(2 min) Served By: #28 CAREY, Neu. Zone Drawn By: NSH #20 VOLCHENKOV"
				if evTeam == outTeams["away"]["abbrev"]:
					evPlayerIds[1] = playerIds["away-" + str(evPlayerJerseys[1])]
				elif evTeam == outTeams["home"]["abbrev"]:
					evPlayerIds[1] = playerIds["home-" + str(evPlayerJerseys[1])]

		if numPlayers == 3:	# Cases where 3 jerseys are listed

			# 3 players are only listed in goals with 2 assists, and for penalties that were served by someone other than the committer
			if evType == "goal":
				if evTeam == outTeams["away"]["abbrev"]:
					evPlayerIds[2] = playerIds["away-" + str(evPlayerJerseys[2])]
				elif evTeam == outTeams["home"]["abbrev"]:
					evPlayerIds[2] = playerIds["home-" + str(evPlayerJerseys[2])]
			elif evType == "penl" and evDesc.lower().find(" served by: ") >= 0: 
				if evTeam == outTeams["away"]["abbrev"]:
					evPlayerIds[2] = playerIds["home-" + str(evPlayerJerseys[2])]
				elif evTeam == outTeams["home"]["abbrev"]:
					evPlayerIds[2] = playerIds["away-" + str(evPlayerJerseys[2])]

		htmlEvents[htmlId]["evPlayers"] = evPlayerIds
		htmlEvents[htmlId]["evPlayersId"] = str(sorted(evPlayerIds))	# A string of the list of sorted playerIds: "[###, ###, ###]"

		#
		# Get playerIds of home/away skaters and goalies
		#

		tds = r.find_all("td", class_=re.compile("bborder")) 
		onIce = [tds[6], tds[7]]

		htmlEvents[htmlId]["awaySkaters"] = []
		htmlEvents[htmlId]["homeSkaters"] = []
		htmlEvents[htmlId]["awayGoalie"] = -1
		htmlEvents[htmlId]["homeGoalie"] = -1

		for i, td in enumerate(onIce):

			onIceSkaters = []
			onIceTeam = ""
			if i == 0:
				onIceTeam = "away"
			elif i == 1:
				onIceTeam = "home"

			for player in td.find_all(attrs={"style" : "cursor:hand;"}):
				position = player["title"][0:player["title"].find(" - ")].lower()
				playerId = playerIds[onIceTeam + "-" + player.text]
				if position in ["right wing", "left wing", "center", "defense"]:
					htmlEvents[htmlId][onIceTeam + "Skaters"].append(playerId)
				elif position == "goalie":
					htmlEvents[htmlId][onIceTeam + "Goalie"] = playerId

		#
		# Get the zone in which the event occurred - always use the home team's perspective
		#

		evZone = None
		if evType == "block":
			if evTeam == outTeams["home"]["abbrev"] and evDesc.lower().find("off. zone") >= 0:		# home team took shot, blocked by away team in away team's off. zone
				evZone = "d"
			elif evTeam == outTeams["away"]["abbrev"]  and evDesc.lower().find("def. zone") >= 0:	# away team took shot, blocked by home team in home team's def. zone
				evZone = "d"
			elif evTeam == outTeams["home"]["abbrev"]  and evDesc.lower().find("def. zone") >= 0:	# home team took shot, blocked by away team in away team's def. zone
				evZone = "o"
			elif evTeam == outTeams["away"]["abbrev"]  and evDesc.lower().find("off. zone") >= 0:	# away team took shot, blocked by home team in home team's off. zone
				evZone = "o"
			elif evDesc.lower().find("neu. zone") >= 0:
				evZone = "n"
		else: 
			if evTeam == outTeams["home"]["abbrev"]  and evDesc.lower().find("off. zone") >= 0:		# home team created event (excluding blocked shot) in home team's off. zone
				evZone = "o"
			elif evTeam == outTeams["away"]["abbrev"]  and evDesc.lower().find("def. zone") >= 0:
				evZone = "o"
			elif evTeam == outTeams["home"]["abbrev"]  and evDesc.lower().find("def. zone") >= 0:
				evZone = "d"
			elif evTeam == outTeams["away"]["abbrev"]  and evDesc.lower().find("off. zone") >= 0:
				evZone = "d"
			elif evDesc.lower().find("neu. zone") >= 0:
				evZone = "n"
		htmlEvents[htmlId]["zone"] = evZone

	#
	#
	# Append on-ice skater data to the json event data
	# Match on period, time, event-type, and event-players
	# To simplify the event-players matching (since the json and html files list event players in different orders for some events, like blocked shots)
	#	simply sort the playerIds in the html and json data and compare the arrays (e.g., by creating 2 strings from the ids)
	#	this saves us from having to check if every single event in the html and json files list players in the same order
	#
	#

	# Prepare dictionary of events for output
	outEvents = dict()

	# Dictionary to map the event types found in the html file --> event types found in the json file
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
	evTypes["chl"] = ""				# league challenge - challenges don't look like they're captured in the json

	for jEv in events:

		# Information needed to match the json and html events
		jPer = jEv["about"]["period"]
		jTime = toSecs(jEv["about"]["periodTime"])
		jType = jEv["result"]["eventTypeId"].lower()
		jPlayers = []
		jPlayerRoles = []
		jPlayersIdList = []
		for jP in jEv["players"]:
			jPlayers.append(jP["player"]["id"])
			jPlayerRoles.append(jP["playerType"])

			# Store a list of playerIds that we'll use to compare to the html list of event players
			# We're creating a separate list (i.e., not reusing jPlayers) because jPlayersIdList will be tailored to match the html descriptions:
			# 	For saved shots, the html file doesn't include the goalie in the description, so we ignore the goalie from jPlayersIdList
			if jType != "shot" or (jType == "shot" and jP["playerType"].lower() != "goalie"):
				jPlayersIdList.append(jP["player"]["id"])

		jPlayersId = str(sorted(jPlayersIdList))

		# Other information to output
		jId = jEv["about"]["eventIdx"]
		jCoords = jEv["coordinates"]
		jDesc = jEv["result"]["description"]
		jAwayGoals = jEv["about"]["goals"]["away"]
		jHomeGoals = jEv["about"]["goals"]["home"]
		jPeriodType = jEv["about"]["periodType"].lower()

		# Information that will be taken from the html events
		jAwaySkaterCount = None
		jHomeSkaterCount = None
		jAwaySkaters = None
		jHomeSkaters = None
		jAwayGoalie = None
		jHomeGoalie = None
		jZone = None

		# The json event also contains a team, but using the html team saves us from checking that the eventTeam is determined the same way 
		# One difference: for blocked shots, the json team is the team who made the block; the html team is the team who took the shot
		jTeam = None 

		# Find the matching event in the html events
		found = False
		for hEv in htmlEvents:
			if found == True:
				break
			else:

				# Used to check events that couldn't be matched
				# if jId == 3:
				# 	print str(htmlEvents[hEv]["period"]) + " -- " + str(jPer)
				# 	print str(htmlEvents[hEv]["time"]) + " -- " + str(jTime)
				# 	print htmlEvents[hEv]["type"] + " -- " + jType
				# 	print htmlEvents[hEv]["evPlayersId"] + " -- " + jPlayersId

				if htmlEvents[hEv]["period"] == jPer and htmlEvents[hEv]["time"] == jTime and evTypes[htmlEvents[hEv]["type"]] == jType and htmlEvents[hEv]["evPlayersId"] == jPlayersId:
					found = True
					jAwaySkaterCount = len(htmlEvents[hEv]["awaySkaters"])
					jHomeSkaterCount = len(htmlEvents[hEv]["homeSkaters"])
					jAwaySkaters = htmlEvents[hEv]["awaySkaters"]
					jHomeSkaters = htmlEvents[hEv]["homeSkaters"]
					jAwayGoalie = htmlEvents[hEv]["awayGoalie"]
					jHomeGoalie = htmlEvents[hEv]["homeGoalie"]
					jZone = htmlEvents[hEv]["zone"]
					jTeam = htmlEvents[hEv]["team"]

					# Create a "matched" flag to check results
					htmlEvents[hEv]["matched"] = "matched"

		# Print json unmatched events
		if found == False:
			print "Unmatched json " + str(jId) + ": " + jDesc

		#
		# Store event information for output
		#

		outEvents[jId] = dict()
		outEvents[jId]["period"] = jPer
		outEvents[jId]["periodType"] = jPeriodType
		outEvents[jId]["time"] = jTime
		outEvents[jId]["description"] = jDesc
		outEvents[jId]["type"] = jType
		outEvents[jId]["team"] = jTeam
		outEvents[jId]["zone"] = jZone

		# For goals, the json includes the goal itself in the score situation
		# But it's more accurate to say that the first goal was scored when it was 0-0
		if jType == "goal":
			if jTeam == outTeams["away"]["abbrev"]:
				outEvents[jId]["aScore"] = jAwayGoals - 1
				outEvents[jId]["hScore"] = jHomeGoals	
			elif jTeam == outTeams["home"]["abbrev"]:
				outEvents[jId]["aScore"] = jAwayGoals
				outEvents[jId]["hScore"] = jHomeGoals - 1	
		else:
			outEvents[jId]["aScore"] = jAwayGoals
			outEvents[jId]["hScore"] = jHomeGoals

		outEvents[jId]["locX"] = None
		outEvents[jId]["locY"] = None
		if len(jCoords) == 2:
			outEvents[jId]["locX"] = jCoords["x"]
			outEvents[jId]["locY"] = jCoords["y"]

		if jPlayers:
			for i, pl in enumerate(jPlayers):
				outEvents[jId]["p" + str(i+1)] = pl
				outEvents[jId]["p" + str(i+1) + "Role"] = jPlayerRoles[i]

		outEvents[jId]["aSkaters"] = jAwaySkaterCount
		if jAwaySkaters:
			for i, sk in enumerate(jAwaySkaters):
				outEvents[jId]["aS" + str(i+1)] = sk

		outEvents[jId]["hSkaters"] = jHomeSkaterCount
		if jHomeSkaters:
			for i, sk in enumerate(jHomeSkaters):
				outEvents[jId]["hS" + str(i+1)] = sk

		outEvents[jId]["aG"] = jAwayGoalie
		outEvents[jId]["hG"] = jHomeGoalie

	# Print unmatched html events
	for hEv in htmlEvents:
		if "matched" not in htmlEvents[hEv]:
			print "Unmatched html " + str(hEv) + ": " + htmlEvents[hEv]["desc"]

	#
	#
	# Loop through events and increment players' stats
	#
	#

	# Dictionary that translates the json eventTypes into the db column names
	# We don't care about json eventTypes like giveaways, takeaways, hits, etc. - we're not tracking these stats
	statAbbrevs = dict()
	statAbbrevs["goal"] = "g"
	statAbbrevs["shot"] = "s"
	statAbbrevs["missed_shot"] = "ms"
	statAbbrevs["blocked_shot"] = "bs"
	statAbbrevs["faceoff"] = "fo"
	statAbbrevs["penalty"] = "pen"

	for ev in outEvents:

		# We only care the stats are a dict key in statAbbrevs
		if outEvents[ev]["type"] in statAbbrevs:

			# pprint(outEvents[ev])

			#
			# Get the score and strength situation for each team
			#
			
			teamScoreSits = dict()
			teamScoreSits[outTeams["away"]["abbrev"]] = outEvents[ev]["aScore"] - outEvents[ev]["hScore"]
			teamScoreSits[outTeams["home"]["abbrev"]] = outEvents[ev]["hScore"] - outEvents[ev]["aScore"]

			oppScoreSits = dict()
			oppScoreSits[outTeams["away"]["abbrev"]] = outEvents[ev]["hScore"] - outEvents[ev]["aScore"]
			oppScoreSits[outTeams["home"]["abbrev"]] = outEvents[ev]["aScore"] - outEvents[ev]["hScore"]

			teamStrengthSits = dict()
			oppStrengthSits = dict()
			if outEvents[ev]["aG"] is None:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "ownGOff"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "oppGOff"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "oppGOff"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "ownGOff"
			elif outEvents[ev]["hG"] is None:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "oppGOff"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "ownGOff"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "ownGOff"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "oppGOff"
			elif outEvents[ev]["aSkaters"] - outEvents[ev]["hSkaters"] > 0:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "pp"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "pk"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "pk"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "pp"
			elif outEvents[ev]["hSkaters"] - outEvents[ev]["aSkaters"] > 0:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "pk"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "pp"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "pp"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "pk"
			elif outEvents[ev]["hSkaters"] == outEvents[ev]["aSkaters"]:
				if outEvents[ev]["hSkaters"] == 5:
					teamStrengthSits[outTeams["away"]["abbrev"]] = "ev5"
					teamStrengthSits[outTeams["home"]["abbrev"]] = "ev5"
					oppStrengthSits[outTeams["away"]["abbrev"]] = "ev5"
					oppStrengthSits[outTeams["home"]["abbrev"]] = "ev5"
				elif outEvents[ev]["hSkaters"] == 4:
					teamStrengthSits[outTeams["away"]["abbrev"]] = "ev4"
					teamStrengthSits[outTeams["home"]["abbrev"]] = "ev4"
					oppStrengthSits[outTeams["away"]["abbrev"]] = "ev4"
					oppStrengthSits[outTeams["home"]["abbrev"]] = "ev4"
				elif outEvents[ev]["hSkaters"] == 3:
					teamStrengthSits[outTeams["away"]["abbrev"]] = "ev3"
					teamStrengthSits[outTeams["home"]["abbrev"]] = "ev3"
					oppStrengthSits[outTeams["away"]["abbrev"]] = "ev3"
					oppStrengthSits[outTeams["home"]["abbrev"]] = "ev3"

			#
			# Increment individual stats
			#

			evType = outEvents[ev]["type"]
			evTeam = outEvents[ev]["team"]

			if evType == "goal":
				outPlayers[outEvents[ev]["p1"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ig"] += 1
				outPlayers[outEvents[ev]["p1"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
				if "p2" in outEvents[ev] and "p2Role" in outEvents[ev]:
					outPlayers[outEvents[ev]["p2"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ia1"] += 1
				if "p3" in outEvents[ev] and "p3Role" in outEvents[ev]:
					outPlayers[outEvents[ev]["p2"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ia2"] += 1
			elif evType in ["shot", "missed_shot", "blocked_shot"]:
				for p in ["p1", "p2", "p3"]:
					if p in outEvents[ev] and p + "Role" in outEvents[ev]:
						if outEvents[ev][p + "Role"].lower() == "shooter":
							outPlayers[outEvents[ev][p]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
						elif outEvents[ev][p + "Role"].lower() == "blocker":
							outPlayers[outEvents[ev][p]][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["blocked"] += 1
			elif evType == "penalty":
				for p in ["p1", "p2", "p3"]:
					if p in outEvents[ev] and p + "Role" in outEvents[ev]:
						if outEvents[ev][p + "Role"].lower() == "drewby":
							outPlayers[outEvents[ev][p]][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["penDrawn"] += 1
						elif outEvents[ev][p + "Role"].lower() == "penaltyon":
							outPlayers[outEvents[ev][p]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["penTaken"] += 1
			elif evType == "faceoff":
				for p in ["p1", "p2", "p3"]:
					if p in outEvents[ev] and p + "Role" in outEvents[ev]:
						if outEvents[ev][p + "Role"].lower() == "winner":
							outPlayers[outEvents[ev][p]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["foWon"] += 1
						elif outEvents[ev][p + "Role"].lower() == "loser":
							outPlayers[outEvents[ev][p]][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["foLost"] += 1

	# In the new events DB table
	# record event-players like this:
	# p1, p2, p3, p1Role, p2Role, p3Role (where the roles are read directly from the json)