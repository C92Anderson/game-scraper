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

	#
	#
	# Prepare team output
	#
	#

	scoreSits = [-3, -2, -1, 0, 1, 2, 3]
	strengthSits = ["ownGOff", "oppGOff", "pk", "pp", "ev5", "ev4", "ev3"]
	
	teamIceSits = dict()	# translates the team abbreviation to 'home' or 'away'
	outTeams = dict()		# dictionary to store team information for output

	for iceSit in teams:	# iceSit = 'home' or 'away'

		outTeams[iceSit] = dict()
		outTeams[iceSit]["abbrev"] = teams[iceSit]["abbreviation"].lower()	# team name abbreviation
		outTeams[iceSit]["playerIds"] = []									# list of playerIds

		teamIceSits[outTeams[iceSit]["abbrev"]] = iceSit
		
	# Create a 'playerIds' dictionary that translates team+jersey (used in the pbp html file) to playerIds
	# Keys: 'home-##' and 'away-##' where ## are jersey numbers
	# Values: playerIds
	playerIds = dict()
	rosters = jsonDict["liveData"]["boxscore"]["teams"]
	for iceSit in rosters:							# 'iceSit' will be 'home' or 'away'
		for player in rosters[iceSit]["players"]:		# 'player' will be 'ID#' where # is a playerId

			key = iceSit + "-" + rosters[iceSit]["players"][player]["jerseyNumber"]
			playerIds[key] = int(rosters[iceSit]["players"][player]["person"]["id"])

			outTeams[iceSit]["playerIds"].append(int(rosters[iceSit]["players"][player]["person"]["id"]))	# Append playerId to the appropriate team

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

		#
		# Parse the event description to produce the same roles found in the json
		#

		roles = dict()

		if evType == "fac":
			aTaker = evDesc.split("#")[1]				# The away FO taker is always listed first
			aTaker = aTaker[0:aTaker.find(" ")]
			hTaker = evDesc.split("#")[2]				# The home FO taker is always listed first
			hTaker = hTaker[0:hTaker.find(" ")]

			if evTeam == outTeams["away"]["abbrev"]:
				roles["winner"] = "away-" + aTaker
				roles["loser"] = "home-" + hTaker
			elif evTeam == outTeams["home"]["abbrev"]:
				roles["winner"] = "home-" + hTaker
				roles["loser"] = "away-" + aTaker

		elif evType in ["shot", "miss"]:
			
			shooter = evDesc.split("#")[1]				# Only a single player is listed for shots-on-goal and misses
			shooter = shooter[0:shooter.find(" ")]

			if evTeam == outTeams["away"]["abbrev"]:
				shooter = "away-" + shooter
			elif evTeam == outTeams["home"]["abbrev"]:
				shooter = "home-" + shooter
			roles["shooter"] = shooter

		elif evType == "block":
			
			shooter = evDesc.split("#")[1]				# The shooter is always listed first
			shooter = shooter[0:shooter.find(" ")]
			blocker = evDesc.split("#")[2]				# The blocker is always listed first
			blocker = blocker[0:blocker.find(" ")]

			if evTeam == outTeams["away"]["abbrev"]:
				roles["shooter"] = "away-" + shooter
				roles["blocker"] = "home-" + blocker
			elif evTeam == outTeams["home"]["abbrev"]:
				roles["shooter"] = "home-" + shooter
				roles["blocker"] = "away-" + blocker

		elif evType in ["give", "take"]:

			player = evDesc.split("#")[1]				# Only a single player is listed for giveaways and takeaway
			player = player[0:player.find(" ")]

			if evTeam == outTeams["away"]["abbrev"]:
				player = "away-" + player
			elif evTeam == outTeams["home"]["abbrev"]:
				player = "home-" + player

			if evType == "give":
				roles["giver"] = player
			elif evType == "take":
				roles["taker"] = player

		elif evType == "goal":

			numPlayers = evDesc.count("#")

			if numPlayers >= 1:
				scorer = evDesc.split("#")[1]				# Scorer is always listed first
				scorer = scorer[0:scorer.find(" ")]
				if evTeam == outTeams["away"]["abbrev"]:
					scorer = "away-" + scorer
				elif evTeam == outTeams["home"]["abbrev"]:
					scorer = "home-" + scorer
				roles["scorer"] = scorer

			if numPlayers >= 2:
				a1 = evDesc.split("#")[2]				# Primary assister is always listed second
				a1 = a1[0:a1.find(" ")]
				if evTeam == outTeams["away"]["abbrev"]:
					a1 = "away-" + a1
				elif evTeam == outTeams["home"]["abbrev"]:
					a1 = "home-" + a1
				roles["assist1"] = a1

			if numPlayers >= 3:
				a2 = evDesc.split("#")[3]				# Secondary assister is always listed second
				a2 = a2[0:a2.find(" ")]
				if evTeam == outTeams["away"]["abbrev"]:
					a2 = "away-" + a2
				elif evTeam == outTeams["home"]["abbrev"]:
					a2 = "home-" + a2
				roles["assist2"] = a2

		elif evType == "hit":

			hitter = evDesc.split("#")[1]				# Hitter is always listed first
			hitter = hitter[0:hitter.find(" ")]
			hittee = evDesc.split("#")[2]
			hittee = hittee[0:hittee.find(" ")]

			if evTeam == outTeams["away"]["abbrev"]:
				roles["hitter"] = "away-" + hitter
				roles["hittee"] = "home-" + hittee
			elif evTeam == outTeams["home"]["abbrev"]:
				roles["hitter"] = "home-" + hitter
				roles["hittee"] = "away-" + hittee

		elif evType == "penl":

			# Get the content between the 1st and 2nd spaces
			# If a player took the penalty, then it will return #XX
			# If a team took the penalty, then it will return 'TEAM'
			penaltyOn = evDesc.split(" ")[1]
			poundIdx = penaltyOn.find("#")
			if poundIdx >= 0:
				penaltyOn = penaltyOn[poundIdx+1:]
				if evTeam == outTeams["away"]["abbrev"]:
					penaltyOn = "away-" + penaltyOn
				elif evTeam == outTeams["home"]["abbrev"]:
					penaltyOn = "home-" + penaltyOn
			else:
				penaltyOn = None

			# Get the player who drew the penalty
			drawnBy = None
			pattern = "Drawn By: "
			drawnByIdx = evDesc.find(pattern)
			if drawnByIdx >= 0:								# Only search for the pattern if it exists
				drawnBy = evDesc[evDesc.find(pattern):]		# Returns a substring *starting* with the pattern
				drawnBy = drawnBy[len(pattern):]			# Remove the pattern from the substring
				drawnBy = drawnBy[drawnBy.find("#")+1:]		# Remove the team abbreviation and "#" from the beginning of the string											
				drawnBy = drawnBy[0:drawnBy.find(" ")]		# Isolate the jersey number

				if evTeam == outTeams["away"]["abbrev"]:	# The penalty-drawer is always on the opposite team of the penalty-taker
					drawnBy = "home-" + drawnBy
				elif evTeam == outTeams["home"]["abbrev"]:
					drawnBy = "away-" + drawnBy

			# Get the player who served the penalty
			servedBy = None
			pattern = "Served By: #"
			servedByIdx = evDesc.find(pattern)
			if servedByIdx >= 0:							# Only search for the pattern if it exists
				servedBy = evDesc[evDesc.find(pattern):]	# Returns a substring *starting* with the pattern
				servedBy = servedBy[len(pattern):]			# Remove the pattern from the substring
				servedBy = servedBy[0:servedBy.find(" ")]	# Isolate the jersey number

				if evTeam == outTeams["away"]["abbrev"]:
					servedBy = "away-" + servedBy
				elif evTeam == outTeams["home"]["abbrev"]:
					servedBy = "home-" + servedBy

			# In the json file, if it's a too many men bench minor,
			#	the description looks like this: "Too many men/ice served by Sam Gagner"
			#	and Sam Gagner is given playerType "PenaltyOn"
			# To replicate this with the html data, do the following:
			if evDesc.lower().find("too many") >= 0 and penaltyOn is None and servedBy is not None:
				penaltyOn = servedBy
				servedBy = None

			# Don't record roles with no player assigned
			if penaltyOn is not None:
				roles["penaltyon"] = penaltyOn
			if servedBy is not None:
				roles["servedby"] = servedBy
			if drawnBy is not None:
				roles["drewby"] = drawnBy

		#
		# Convert jersey numbers into playerIds and store the dict
		#

		for role in roles:
			roles[role] = playerIds[roles[role]]

		htmlEvents[htmlId]["roles"] = roles

		#
		# Get playerIds of home/away skaters and goalies
		#

		tds = r.find_all("td", class_=re.compile("bborder")) 
		onIce = [tds[6], tds[7]]

		htmlEvents[htmlId]["awaySkaters"] = []
		htmlEvents[htmlId]["homeSkaters"] = []

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

		evHZone = None
		if evType == "block":
			if evTeam == outTeams["home"]["abbrev"] and evDesc.lower().find("off. zone") >= 0:		# home team took shot, blocked by away team in away team's off. zone
				evHZone = "d"
			elif evTeam == outTeams["away"]["abbrev"] and evDesc.lower().find("def. zone") >= 0:	# away team took shot, blocked by home team in home team's def. zone
				evHZone = "d"
			elif evTeam == outTeams["home"]["abbrev"] and evDesc.lower().find("def. zone") >= 0:	# home team took shot, blocked by away team in away team's def. zone
				evHZone = "o"
			elif evTeam == outTeams["away"]["abbrev"] and evDesc.lower().find("off. zone") >= 0:	# away team took shot, blocked by home team in home team's off. zone
				evHZone = "o"
			elif evDesc.lower().find("neu. zone") >= 0:
				evHZone = "n"
		else: 
			if evTeam == outTeams["home"]["abbrev"] and evDesc.lower().find("off. zone") >= 0:		# home team created event (excluding blocked shot) in home team's off. zone
				evHZone = "o"
			elif evTeam == outTeams["away"]["abbrev"] and evDesc.lower().find("def. zone") >= 0:
				evHZone = "o"
			elif evTeam == outTeams["home"]["abbrev"] and evDesc.lower().find("def. zone") >= 0:
				evHZone = "d"
			elif evTeam == outTeams["away"]["abbrev"] and evDesc.lower().find("off. zone") >= 0:
				evHZone = "d"
			elif evDesc.lower().find("neu. zone") >= 0:
				evHZone = "n"
		htmlEvents[htmlId]["hZone"] = evHZone

	#
	#
	# Append on-ice skater data to the json event data
	# Match on period, time, event-type, and event-players/roles
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
		jDesc = jEv["result"]["description"]
		
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

			if jType == "giveaway":
				role = "giver"
			elif jType == "takeaway":
				role = "taker"
			elif jType == "goal":

				pattern = "assists: "
				assistersIdx = jDesc.lower().find(pattern)
				noAssistsIdx = jDesc.lower().find("assists: none")
				if assistersIdx >= 0 and noAssistsIdx < 0:
					a1String = None
					a2String = None
					assistersString = jDesc[jDesc.lower().find(pattern):]
					pattern = "), "
					commaIdx = assistersString.find(pattern)
					if commaIdx < 0: 	# 1 assister
						a1String = assistersString
					elif commaIdx >= 0:	# 2 assisters
						a1String = assistersString.split(",")[0] # This substring contains the full name of the primary assister
						a2String = assistersString.split(",")[1] # This substring contains the full name of the secondary assister

					# Look for jP's full name in the a1 and a2 strings to see if jP has role assist1 or assist2
					if a1String is not None and a1String.lower().find(jP["player"]["fullName"].lower()) >= 0:
						role = "assist1"
					elif a2String is not None and a2String.lower().find(jP["player"]["fullName"].lower()) >= 0:
						role = "assist2"

			jRoles[role] = jP["player"]["id"]
		
		if jType == "shot":
			del jRoles["goalie"]
		elif jType == "penalty":
			if jDesc.lower().find("puck over glass") >= 0:
				if "servedby" not in jRoles and "drewby" in jRoles:
					jRoles["servedby"] = jRoles["drewby"]
					del jRoles["drewby"]

		# Other information to output
		jId = jEv["about"]["eventIdx"]
		jCoords = jEv["coordinates"]
		
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
		jHZone = None

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
				# 	print htmlEvents[hEv]["roles"] + " -- " + jRoles

				if htmlEvents[hEv]["period"] == jPer and htmlEvents[hEv]["time"] == jTime and evTypes[htmlEvents[hEv]["type"]] == jType and htmlEvents[hEv]["roles"] == jRoles:
					found = True
					jAwaySkaterCount = len(htmlEvents[hEv]["awaySkaters"])
					jHomeSkaterCount = len(htmlEvents[hEv]["homeSkaters"])
					jAwaySkaters = htmlEvents[hEv]["awaySkaters"]
					jHomeSkaters = htmlEvents[hEv]["homeSkaters"]
					jHZone = htmlEvents[hEv]["hZone"]
					jTeam = htmlEvents[hEv]["team"]

					if "awayGoalie" in htmlEvents[hEv]:
						jAwayGoalie = htmlEvents[hEv]["awayGoalie"]
					if "homeGoalie" in htmlEvents[hEv]:
						jHomeGoalie = htmlEvents[hEv]["homeGoalie"]

					# Create a "matched" flag to check results
					htmlEvents[hEv]["matched"] = "matched"

		# Print unmatched json events
		if found == False:
			print "Unmatched json event " + str(jId) + ": " + jDesc

		#
		#
		# Store event information for output
		#
		#

		outEvents[jId] = dict()
		outEvents[jId]["period"] = jPer
		outEvents[jId]["periodType"] = jPeriodType
		outEvents[jId]["time"] = jTime
		outEvents[jId]["description"] = jDesc
		outEvents[jId]["type"] = jType
		outEvents[jId]["team"] = jTeam
		outEvents[jId]["hZone"] = jHZone
		outEvents[jId]["roles"] = jRoles

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

		outEvents[jId]["aSkaterCount"] = jAwaySkaterCount
		outEvents[jId]["aSkaters"] = jAwaySkaters

		outEvents[jId]["hSkaterCount"] = jHomeSkaterCount
		outEvents[jId]["hSkaters"] = jHomeSkaters

		if jAwayGoalie and jAwayGoalie is not None:
			outEvents[jId]["aG"] = jAwayGoalie
		if jHomeGoalie and jHomeGoalie is not None:
			outEvents[jId]["hG"] = jHomeGoalie

	# Print unmatched html events
	for hEv in htmlEvents:
		if "matched" not in htmlEvents[hEv]:
			print "Unmatched html event " + str(hEv) + ": " + htmlEvents[hEv]["desc"]

	#
	#
	# Loop through events and increment players' stats
	#
	#

	for ev in outEvents:
		if outEvents[ev]["type"] in ["goal", "shot", "missed_shot", "blocked_shot", "faceoff", "penalty"]:

			#
			# Get the score and strength situation for each team
			#
			
			teamScoreSits = dict()	# Returns the score situation from the key-team's perspective
			teamScoreSits[outTeams["away"]["abbrev"]] = outEvents[ev]["aScore"] - outEvents[ev]["hScore"]
			teamScoreSits[outTeams["home"]["abbrev"]] = outEvents[ev]["hScore"] - outEvents[ev]["aScore"]

			oppScoreSits = dict()	# Returns the score situation from the key-team's opponent perspective
			oppScoreSits[outTeams["away"]["abbrev"]] = outEvents[ev]["hScore"] - outEvents[ev]["aScore"]
			oppScoreSits[outTeams["home"]["abbrev"]] = outEvents[ev]["aScore"] - outEvents[ev]["hScore"]

			teamStrengthSits = dict()	# Returns the strength situation from the key-team's perspective
			oppStrengthSits = dict()	# Returns the strength situation from the key-team's opponent perspective
			if "aG" not in outEvents[ev] or outEvents[ev]["aG"] is None:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "ownGOff"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "oppGOff"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "oppGOff"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "ownGOff"
			elif "hG" not in outEvents[ev] or outEvents[ev]["hG"] is None:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "oppGOff"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "ownGOff"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "ownGOff"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "oppGOff"
			elif outEvents[ev]["aSkaterCount"] - outEvents[ev]["hSkaterCount"] > 0:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "pp"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "pk"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "pk"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "pp"
			elif outEvents[ev]["hSkaterCount"] - outEvents[ev]["aSkaterCount"] > 0:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "pk"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "pp"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "pp"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "pk"
			elif outEvents[ev]["hSkaterCount"] == outEvents[ev]["aSkaterCount"]:
				if outEvents[ev]["hSkaterCount"] == 5:
					teamStrengthSits[outTeams["away"]["abbrev"]] = "ev5"
					teamStrengthSits[outTeams["home"]["abbrev"]] = "ev5"
					oppStrengthSits[outTeams["away"]["abbrev"]] = "ev5"
					oppStrengthSits[outTeams["home"]["abbrev"]] = "ev5"
				elif outEvents[ev]["hSkaterCount"] == 4:
					teamStrengthSits[outTeams["away"]["abbrev"]] = "ev4"
					teamStrengthSits[outTeams["home"]["abbrev"]] = "ev4"
					oppStrengthSits[outTeams["away"]["abbrev"]] = "ev4"
					oppStrengthSits[outTeams["home"]["abbrev"]] = "ev4"
				elif outEvents[ev]["hSkaterCount"] == 3:
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
				outPlayers[outEvents[ev]["roles"]["scorer"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ig"] += 1
				outPlayers[outEvents[ev]["roles"]["scorer"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
				if "assist1" in outEvents[ev]["roles"]:
					outPlayers[outEvents[ev]["roles"]["assist1"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ia1"] += 1
				if "assist2" in outEvents[ev]["roles"]:
					outPlayers[outEvents[ev]["roles"]["assist2"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ia2"] += 1
			elif evType in ["shot", "missed_shot"]:
				outPlayers[outEvents[ev]["roles"]["shooter"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
			elif evType == "blocked_shot":
				outPlayers[outEvents[ev]["roles"]["shooter"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["is"] += 1
				outPlayers[outEvents[ev]["roles"]["blocker"]][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["blocked"] += 1
			elif evType == "penalty":
				if "drewby" in outEvents[ev]["roles"]:
					outPlayers[outEvents[ev]["roles"]["drewby"]][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["penDrawn"] += 1
				if "penaltyon" in outEvents[ev]["roles"]:
					outPlayers[outEvents[ev]["roles"]["penaltyon"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["penTaken"] += 1
			elif evType == "faceoff":
				outPlayers[outEvents[ev]["roles"]["winner"]][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["foWon"] += 1
				outPlayers[outEvents[ev]["roles"]["loser"]][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["foLost"] += 1

			#
			# Increment stats for on-ice players
			#

			hPlayers = []
			if "hSkaters" in outEvents[ev]:
				hPlayers.extend(outEvents[ev]["hSkaters"])
			if "hG" in outEvents[ev]:
				hPlayers.append(outEvents[ev]["hG"])

			for pId in hPlayers:
				if evType == "goal":
					if evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["gf"] += 1
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					elif evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ga"] += 1
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sa"] += 1
				elif evType == "shot":
					if evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					elif evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sa"] += 1
				elif evType == "missed_shot":
					if evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["msf"] += 1
					elif evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["msa"] += 1
				elif evType == "blocked_shot":
					if evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["bsf"] += 1
					elif evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["bsa"] += 1
				elif evType == "faceoff":
					# For face-offs, we don't care who won (the evTeam) - we're just tracking how many o/d/n FOs the player was on the ice for
					evHZone = outEvents[ev]["hZone"]
					outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]][evHZone + "fo"] += 1

			aPlayers = []
			if "aSkaters" in outEvents[ev]:
				aPlayers.extend(outEvents[ev]["aSkaters"])
			if "aG" in outEvents[ev]:
				aPlayers.append(outEvents[ev]["aG"])

			for pId in aPlayers:
				print pId
				if evType == "goal":
					if evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["gf"] += 1
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					elif evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["ga"] += 1
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sa"] += 1
				elif evType == "shot":
					if evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					elif evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sa"] += 1
				elif evType == "missed_shot":
					if evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["msf"] += 1
					elif evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["msa"] += 1
				elif evType == "blocked_shot":
					if evTeam == outTeams["away"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["bsf"] += 1
					elif evTeam == outTeams["home"]["abbrev"]:
						outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["bsa"] += 1
				elif evType == "faceoff":
					# For face-offs, we don't care who won (the evTeam) - we're just tracking how many o/d/n FOs the player was on the ice for
					# Since outEvents[ev]["hZone"] is always from the home-team's perspective, we need to flip the o-zone and d-zone for the away-team
					evAZone = "n" 
					if outEvents[ev]["hZone"] == "o":
						evAZone = "d"
					elif outEvents[ev]["hZone"] == "d":
						evAZone = "o"
					outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]][evAZone + "fo"] += 1


				
	pprint(outPlayers)
	# In the new events DB table
	# record event-players like this:
	# p1, p2, p3, p1Role, p2Role, p3Role (where the roles are read directly from the json)