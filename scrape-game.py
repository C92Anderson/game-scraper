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
	print "-----"

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
	strengthSits = ["ownGPulled", "oppGPulled", "pk", "pp", "ev5", "ev4", "ev3"]
	
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

				outTeams[iceSit][strSit][scSit]["toi"] = 0
				outTeams[iceSit][strSit][scSit]["foWon"] = 0	# won face-offs
				outTeams[iceSit][strSit][scSit]["foLost"] = 0	# lost face-offs

				outTeams[iceSit][strSit][scSit]["gf"] = 0
				outTeams[iceSit][strSit][scSit]["ga"] = 0
				outTeams[iceSit][strSit][scSit]["sf"] = 0
				outTeams[iceSit][strSit][scSit]["sa"] = 0
				outTeams[iceSit][strSit][scSit]["bsf"] = 0
				outTeams[iceSit][strSit][scSit]["bsa"] = 0
				outTeams[iceSit][strSit][scSit]["msf"] = 0
				outTeams[iceSit][strSit][scSit]["msa"] = 0

				outTeams[iceSit][strSit][scSit]["ofo"] = 0		# offensive zone face-off
				outTeams[iceSit][strSit][scSit]["dfo"] = 0
				outTeams[iceSit][strSit][scSit]["nfo"] = 0

				outTeams[iceSit][strSit][scSit]["penTaken"] = 0
				outTeams[iceSit][strSit][scSit]["penDrawn"] = 0

	# Create a 'playerIds' dictionary that translates team+jersey (used in the pbp html file) to playerIds
	# Keys: 'home-##' and 'away-##' where ## are jersey numbers
	# Values: playerIds
	playerIds = dict()
	for iceSit in rosters:							# 'iceSit' will be 'home' or 'away'
		for player in rosters[iceSit]["players"]:	# 'player' will be 'ID#' where # is a playerId

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
		outPlayers[pId]["firstName"] = players[pId]["firstName"]
		outPlayers[pId]["lastName"] = players[pId]["lastName"]

		# Initialize stats
		for strSit in strengthSits:
			outPlayers[pId][strSit] = dict()

			for scSit in scoreSits:
				outPlayers[pId][strSit][scSit] = dict()

				outPlayers[pId][strSit][scSit]["toi"] = 0
				outPlayers[pId][strSit][scSit]["ig"] = 0		# individual goals
				outPlayers[pId][strSit][scSit]["is"] = 0
				outPlayers[pId][strSit][scSit]["ibs"] = 0
				outPlayers[pId][strSit][scSit]["ims"] = 0
				outPlayers[pId][strSit][scSit]["ia1"] = 0		# primary assists
				outPlayers[pId][strSit][scSit]["ia2"] = 0		# secondary assists
				outPlayers[pId][strSit][scSit]["blocked"] = 0	# shots that this player blocked
				outPlayers[pId][strSit][scSit]["foWon"] = 0		# individual won face-offs
				outPlayers[pId][strSit][scSit]["foLost"] = 0	# individual lost face-offs

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
		# The json team abbreviations are different from the html ones:
		# N.J (html) -> NJD (json); S.J -> SJS; T.B -> TBL; L.A -> LAK

		evTeam = evDesc[0:evDesc.find(" ")].lower()
		if evTeam == "n.j":
			evTeam = "njd"
		elif evTeam == "s.j":
			evTeam = "sjs"
		elif evTeam == "t.b":
			evTeam = "tbl"
		elif evTeam == "l.a":
			evTeam = "lak"

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

		htmlEvents[htmlId]["aSkaters"] = []
		htmlEvents[htmlId]["hSkaters"] = []

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
					htmlEvents[htmlId][onIceTeam[0] + "Skaters"].append(playerId)
				elif position == "goalie":
					htmlEvents[htmlId][onIceTeam[0] + "G"] = playerId

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
	# Done looping through html events
	#

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

		# Create a dictionary for this event
		jId = jEv["about"]["eventIdx"]
		outEvents[jId] = dict()

		outEvents[jId]["period"] = jEv["about"]["period"]
		outEvents[jId]["periodType"] = jEv["about"]["periodType"].lower()
		outEvents[jId]["time"] = toSecs(jEv["about"]["periodTime"])

		outEvents[jId]["description"] = jEv["result"]["description"]
		outEvents[jId]["type"] = jEv["result"]["eventTypeId"].lower()
		if "secondaryType" in jEv["result"]:
			outEvents[jId]["subtype"] = jEv["result"]["secondaryType"].lower()

		if "coordinates" in jEv and len(jEv["coordinates"]) == 2:
			outEvents[jId]["locX"] = jEv["coordinates"]["x"]
			outEvents[jId]["locY"] = jEv["coordinates"]["y"]

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

			if outEvents[jId]["type"] == "giveaway":
				role = "giver"
			elif outEvents[jId]["type"] == "takeaway":
				role = "taker"
			elif outEvents[jId]["type"] == "goal":
				pattern = "assists: "
				assistersIdx = outEvents[jId]["description"].lower().find(pattern)
				noAssistsIdx = outEvents[jId]["description"].lower().find("assists: none")
				if assistersIdx >= 0 and noAssistsIdx < 0:
					a1String = None
					a2String = None
					assistersString = outEvents[jId]["description"][outEvents[jId]["description"].lower().find(pattern):]
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
		
		if outEvents[jId]["type"] == "shot":
			del jRoles["goalie"]
		elif outEvents[jId]["type"] == "penalty":
			if outEvents[jId]["description"].lower().find("puck over glass") >= 0:
				if "servedby" not in jRoles and "drewby" in jRoles:
					jRoles["servedby"] = jRoles["drewby"]
					del jRoles["drewby"]

		# If there's no roles, we don't want to create a 'roles' key in the event's output dictionary
		if len(jRoles) > 0:
			outEvents[jId]["roles"] = jRoles

		#
		# Done getting player roles
		#

		#
		#
		# Find the corresponding html event so we can record: the event team, event's zone, all on-ice players
		#
		#

		found = False
		for hEv in htmlEvents:
			if found == True:
				break
			else:
				if (htmlEvents[hEv]["period"] == outEvents[jId]["period"]
					and htmlEvents[hEv]["time"] == outEvents[jId]["time"]
					and evTypes[htmlEvents[hEv]["type"]] == outEvents[jId]["type"]
					and htmlEvents[hEv]["roles"] == jRoles):

					found = True
					outEvents[jId]["aSkaterCount"] = len(htmlEvents[hEv]["aSkaters"])
					outEvents[jId]["hSkaterCount"] = len(htmlEvents[hEv]["hSkaters"])
					outEvents[jId]["aSkaters"] = htmlEvents[hEv]["aSkaters"]
					outEvents[jId]["hSkaters"] = htmlEvents[hEv]["hSkaters"]

					if htmlEvents[hEv]["hZone"] is not None:
						outEvents[jId]["hZone"] = htmlEvents[hEv]["hZone"]

					if htmlEvents[hEv]["team"] is not None:
						outEvents[jId]["team"] = htmlEvents[hEv]["team"]

						# Record the iceSit (home/away)
						if outEvents[jId]["team"] == outTeams["home"]["abbrev"]:
							outEvents[jId]["iceSit"] = "home"
						elif outEvents[jId]["team"] == outTeams["away"]["abbrev"]:
							outEvents[jId]["iceSit"] = "away"

					if "aG" in htmlEvents[hEv]:
						outEvents[jId]["aG"] = htmlEvents[hEv]["aG"]
					if "hG" in htmlEvents[hEv]:
						outEvents[jId]["hG"] = htmlEvents[hEv]["hG"]

					# Create a "matched" flag to check results
					htmlEvents[hEv]["matched"] = "matched"

		# Print unmatched json events
		if found == False:
			print "Unmatched json event " + str(jId) + ": " + outEvents[jId]["description"]

		# Record the home and away scores when the event occurred
		# For goals, the json includes the goal itself in the score situation, but it's more accurate to say that the first goal was scored when it was 0-0
		if outEvents[jId]["type"] == "goal":
			if outEvents[jId]["team"] == outTeams["away"]["abbrev"]:
				outEvents[jId]["aScore"] = jEv["about"]["goals"]["away"] - 1
				outEvents[jId]["hScore"] = jEv["about"]["goals"]["home"]	
			elif outEvents[jId]["team"] == outTeams["home"]["abbrev"]:
				outEvents[jId]["aScore"] = jEv["about"]["goals"]["away"]
				outEvents[jId]["hScore"] = jEv["about"]["goals"]["home"] - 1	
		else:
			outEvents[jId]["aScore"] = jEv["about"]["goals"]["away"]
			outEvents[jId]["hScore"] = jEv["about"]["goals"]["home"]

	#
	# Done looping through json events to match events with html events and preparing outEvents
	#

	# Print unmatched html events
	for hEv in htmlEvents:
		if "matched" not in htmlEvents[hEv]:
			print "Unmatched html event " + str(hEv) + ": " + htmlEvents[hEv]["desc"]

	#
	#
	# Loop through events and increment players' stats and teams' stats
	#
	#

	for ev in outEvents:
		if outEvents[ev]["type"] in ["goal", "shot", "missed_shot", "blocked_shot", "faceoff", "penalty"]:

			#
			# Get the score and strength situation for each team
			#
			
			teamScoreSits = dict()	# Returns the score situation from the key-team's perspective
			teamScoreSits[outTeams["away"]["abbrev"]] = max(-3, min(3, outEvents[ev]["aScore"] - outEvents[ev]["hScore"]))
			teamScoreSits[outTeams["home"]["abbrev"]] = max(-3, min(3, outEvents[ev]["hScore"] - outEvents[ev]["aScore"]))

			oppScoreSits = dict()	# Returns the score situation from the key-team's opponent perspective
			oppScoreSits[outTeams["away"]["abbrev"]] = max(-3, min(3, outEvents[ev]["hScore"] - outEvents[ev]["aScore"]))
			oppScoreSits[outTeams["home"]["abbrev"]] = max(-3, min(3, outEvents[ev]["aScore"] - outEvents[ev]["hScore"]))

			teamStrengthSits = dict()	# Returns the strength situation from the key-team's perspective
			oppStrengthSits = dict()	# Returns the strength situation from the key-team's opponent perspective

			if "aG" not in outEvents[ev]:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "ownGPulled"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "oppGPulled"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "oppGPulled"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "ownGPulled"
			elif "hG" not in outEvents[ev]:
				teamStrengthSits[outTeams["away"]["abbrev"]] = "oppGPulled"
				teamStrengthSits[outTeams["home"]["abbrev"]] = "ownGPulled"
				oppStrengthSits[outTeams["away"]["abbrev"]] = "ownGPulled"
				oppStrengthSits[outTeams["home"]["abbrev"]] = "oppGPulled"
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
					# For face-off zone counts, we don't care who won (the evTeam) - we're just tracking how many o/d/n FOs the player was on the ice for
					evHZone = outEvents[ev]["hZone"]
					outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]][evHZone + "fo"] += 1

			aPlayers = []
			if "aSkaters" in outEvents[ev]:
				aPlayers.extend(outEvents[ev]["aSkaters"])
			if "aG" in outEvents[ev]:
				aPlayers.append(outEvents[ev]["aG"])

			for pId in aPlayers:
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
					# For face-off zone counts, we don't care who won (the evTeam) - we're just tracking how many o/d/n FOs the player was on the ice for
					# Since outEvents[ev]["hZone"] is always from the home-team's perspective, we need to flip the o-zone and d-zone for the away-team
					evAZone = outEvents[ev]["hZone"]
					if outEvents[ev]["hZone"] == "o":
						evAZone = "d"
					elif outEvents[ev]["hZone"] == "d":
						evAZone = "o"
					outPlayers[pId][teamStrengthSits[evTeam]][teamScoreSits[evTeam]][evAZone + "fo"] += 1

			#
			# Increment stats for teams
			#

			if evType == "goal":
				if evTeam == outTeams["home"]["abbrev"]:
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["gf"] += 1
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["ga"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["sa"] += 1
				elif evTeam == outTeams["away"]["abbrev"]:
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["gf"] += 1
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["ga"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["sa"] += 1
			elif evType == "shot":
				if evTeam == outTeams["home"]["abbrev"]:
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["sa"] += 1
				elif evTeam == outTeams["away"]["abbrev"]:
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["sf"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["sa"] += 1
			elif evType == "missed_shot":
				if evTeam == outTeams["home"]["abbrev"]:
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["msf"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["msa"] += 1
				elif evTeam == outTeams["away"]["abbrev"]:
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["msf"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["msa"] += 1
			elif evType == "blocked_shot":
				if evTeam == outTeams["home"]["abbrev"]:
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["bsf"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["bsa"] += 1
				elif evTeam == outTeams["away"]["abbrev"]:
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["bsf"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["bsa"] += 1
			elif evType == "penalty":
				if evTeam == outTeams["home"]["abbrev"]:
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["penTaken"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["penDrawn"] += 1
				elif evTeam == outTeams["away"]["abbrev"]:
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["penTaken"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["penDrawn"] += 1
			elif evType == "faceoff":
				# Increment o/d/n faceoffs for the home team
				evHZone = outEvents[ev]["hZone"]
				outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]][evHZone + "fo"] += 1

				# Increment o/d/n faceoffs for the away team
				evAZone = outEvents[ev]["hZone"]
				if outEvents[ev]["hZone"] == "o":
					evAZone = "d"
				elif outEvents[ev]["hZone"] == "d":
					evAZone = "o"
				outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]][evAZone + "fo"] += 1

				# Increment foWon/foLost counts
				if evTeam == outTeams["home"]["abbrev"]:
					outTeams["home"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["foWon"] += 1
					outTeams["away"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["foLost"] += 1
				elif evTeam == outTeams["away"]["abbrev"]:
					outTeams["away"][teamStrengthSits[evTeam]][teamScoreSits[evTeam]]["foWon"] += 1
					outTeams["home"][oppStrengthSits[evTeam]][oppScoreSits[evTeam]]["foLost"] += 1

	#
	# Done looping through outEvents to record player stats
	#

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
				nestedShifts[pId]["firstName"] = s["firstName"]
				nestedShifts[pId]["lastName"] = s["lastName"]
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
		# For each strength situation, store the seconds (as a set) at which the situation occurred
		#

		ownGPulledSecs = dict()
		ownGPulledSecs["away"] = set()
		ownGPulledSecs["home"] = set()

		ppSecs = dict()
		ppSecs["away"] = set()
		ppSecs["home"] = set()

		pkSecs = dict()
		pkSecs["away"] = set()
		pkSecs["home"] = set()

		ev3Secs = set()
		ev4Secs = set()
		ev5Secs = set()

		for sec in range(0, periodDurs[period]):
			
			if aGCountPerSec[sec] == 0:
				ownGPulledSecs["away"].add(sec)
			elif hGCountPerSec[sec] == 0:
				ownGPulledSecs["home"].add(sec)
			elif aSCountPerSec[sec] - hSCountPerSec[sec] > 0:
				ppSecs["away"].add(sec)
				pkSecs["home"].add(sec)
			elif hSCountPerSec[sec] - aSCountPerSec[sec] > 0:
				ppSecs["home"].add(sec)
				pkSecs["away"].add(sec)
			elif aSCountPerSec[sec] == hSCountPerSec[sec]:
				if aSCountPerSec[sec] == 5:
					ev5Secs.add(sec)
				elif aSCountPerSec[sec] == 4:
					ev4Secs.add(sec)
				elif aSCountPerSec[sec] == 3:
					ev3Secs.add(sec)

		#
		# Record the score differential at each second (the list index represents the number of seconds elapsed)
		# The score differential is calculated from the home team's perspective (home - away)
		#

		periodStart = [outEvents[ev] for ev in outEvents if outEvents[ev]["type"] == "period_start" and outEvents[ev]["period"] == period][0]
		goals = [outEvents[ev] for ev in outEvents if outEvents[ev]["type"] == "goal" and outEvents[ev]["period"] == period]

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
			scoreSitSecs[iceSit][-3] = set()
			scoreSitSecs[iceSit][-2] = set()
			scoreSitSecs[iceSit][-1] = set()
			scoreSitSecs[iceSit][-0] = set()
			scoreSitSecs[iceSit][1] = set()
			scoreSitSecs[iceSit][2] = set()
			scoreSitSecs[iceSit][3] = set()

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

			if nestedShifts[pId]["team"] == outTeams["away"]["abbrev"]:	# Record tois for players on the AWAY team
				# For each score situation, increment tois for each strength situation (increment because we're adding this period's toi to previous periods' tois)
				for scoreSit in range(-3, 4):
					outPlayers[pId]["ownGPulled"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ownGPulledSecs["away"], scoreSitSecs["away"][scoreSit]))
					outPlayers[pId]["oppGPulled"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ownGPulledSecs["home"], scoreSitSecs["away"][scoreSit]))
					outPlayers[pId]["pp"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ppSecs["away"], scoreSitSecs["away"][scoreSit]))
					outPlayers[pId]["pk"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], pkSecs["away"], scoreSitSecs["away"][scoreSit]))
					outPlayers[pId]["ev5"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ev5Secs, scoreSitSecs["away"][scoreSit]))
					outPlayers[pId]["ev4"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ev4Secs, scoreSitSecs["away"][scoreSit]))
					outPlayers[pId]["ev3"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ev3Secs, scoreSitSecs["away"][scoreSit]))

			elif nestedShifts[pId]["team"] == outTeams["away"]["abbrev"]:	# Record tois for players on the HOME team
				for scoreSit in range(-3, 4):
					outPlayers[pId]["ownGPulled"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ownGPulledSecs["home"], scoreSitSecs["home"][scoreSit]))
					outPlayers[pId]["oppGPulled"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ownGPulledSecs["away"], scoreSitSecs["home"][scoreSit]))
					outPlayers[pId]["pp"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ppSecs["home"], scoreSitSecs["home"][scoreSit]))
					outPlayers[pId]["pk"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], pkSecs["home"], scoreSitSecs["home"][scoreSit]))
					outPlayers[pId]["ev5"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ev5Secs, scoreSitSecs["home"][scoreSit]))
					outPlayers[pId]["ev4"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ev4Secs, scoreSitSecs["home"][scoreSit]))
					outPlayers[pId]["ev3"][scoreSit]["toi"] += len(set.intersection(nestedShifts[pId][period], ev3Secs, scoreSitSecs["home"][scoreSit]))

		#
		# Increment team toi for each score and strength situation
		#

		for scoreSit in range(-3, 4):

			# Increment HOME team tois
			outTeams["home"]["ownGPulled"][scoreSit]["toi"] += len(set.intersection(ownGPulledSecs["home"], scoreSitSecs["home"][scoreSit]))
			outTeams["home"]["oppGPulled"][scoreSit]["toi"] += len(set.intersection(ownGPulledSecs["away"], scoreSitSecs["home"][scoreSit]))
			outTeams["home"]["pp"][scoreSit]["toi"] += len(set.intersection(ppSecs["home"], scoreSitSecs["home"][scoreSit]))
			outTeams["home"]["pk"][scoreSit]["toi"] += len(set.intersection(pkSecs["home"], scoreSitSecs["home"][scoreSit]))
			outTeams["home"]["ev5"][scoreSit]["toi"] += len(set.intersection(ev5Secs, scoreSitSecs["home"][scoreSit]))
			outTeams["home"]["ev4"][scoreSit]["toi"] += len(set.intersection(ev4Secs, scoreSitSecs["home"][scoreSit]))
			outTeams["home"]["ev3"][scoreSit]["toi"] += len(set.intersection(ev3Secs, scoreSitSecs["home"][scoreSit]))

			# Increment AWAY team tois
			outTeams["away"]["ownGPulled"][scoreSit]["toi"] += len(set.intersection(ownGPulledSecs["away"], scoreSitSecs["away"][scoreSit]))
			outTeams["away"]["oppGPulled"][scoreSit]["toi"] += len(set.intersection(ownGPulledSecs["home"], scoreSitSecs["away"][scoreSit]))
			outTeams["away"]["pp"][scoreSit]["toi"] += len(set.intersection(ppSecs["away"], scoreSitSecs["away"][scoreSit]))
			outTeams["away"]["pk"][scoreSit]["toi"] += len(set.intersection(pkSecs["away"], scoreSitSecs["away"][scoreSit]))
			outTeams["away"]["ev5"][scoreSit]["toi"] += len(set.intersection(ev5Secs, scoreSitSecs["away"][scoreSit]))
			outTeams["away"]["ev4"][scoreSit]["toi"] += len(set.intersection(ev4Secs, scoreSitSecs["away"][scoreSit]))
			outTeams["away"]["ev3"][scoreSit]["toi"] += len(set.intersection(ev3Secs, scoreSitSecs["away"][scoreSit]))

	#
	# Done looping through each period and processing shifts
	#

	#
	#
	# Prepare output files that will be loaded into the database
	#
	#

	print "-----"
	print "Preparing csv files"

	#
	# Output shifts
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-shifts.csv", "w")
	outString = "season,date,gameId,team,playerId,period,start,end\n"
	outFile.write(outString)

	for sh in shifts:
		outString = str(seasonArg)
		outString += "," + str(gameDate)
		outString += "," + str(gameId)
		outString += "," + nestedShifts[sh["playerId"]]["team"]
		outString += "," + str(sh["playerId"])
		outString += "," + str(sh["period"])
		outString += "," + str(toSecs(sh["startTime"]))
		outString += "," + str(toSecs(sh["endTime"]))
		outString += "\n"
		outFile.write(outString)

	outFile.close()

	#
	# Output events
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-events.csv", "w")
	outString = "season,date,gameId,eventId,"
	outString += "period,time,aScore,hScore,aSkaters,hSkaters,locX,locY,"
	outString += "desc,type,subtype,"
	outString += "team,teamIceSit,"	# team is the event team; teamIceSit is home/away for the event team
	outString += "p1,p2,p3,p1Role,p2Role,p3Role,"
	outString += "as1,as2,as3,as4,as5,as6,ag,"
	outString += "hs1,hs2,hs3,hs4,hs5,hs6,hg\n"
	outFile.write(outString)

	for ev in outEvents:

		outString = str(seasonArg)
		outString += "," + str(gameDate)
		outString += "," + str(gameId)
		outString += "," + str(ev)

		outString += "," + str(outEvents[ev]["period"])
		outString += "," + str(outEvents[ev]["time"])
		outString += "," + str(outEvents[ev]["aScore"])
		outString += "," + str(outEvents[ev]["hScore"])
		outString += "," + outputVal(outEvents[ev], "aSkaterCount")
		outString += "," + outputVal(outEvents[ev], "hSkaterCount")
		outString += "," + outputVal(outEvents[ev], "locX")
		outString += "," + outputVal(outEvents[ev], "locY")

		outString += "," + outEvents[ev]["description"].replace(",", ";") # Replace commas to maintain the csv structure
		outString += "," + outEvents[ev]["type"]
		outString += "," + outputVal(outEvents[ev], "subtype")

		outString += "," + outputVal(outEvents[ev], "team")
		outString += "," + outputVal(outEvents[ev], "iceSit")

		#
		# Process roles
		#

		if "roles" not in outEvents[ev]:
			outString += ",NULL,NULL,NULL,NULL,NULL,NULL"
		else:
			pIdString = ""
			roleString = ""

			# Append playerIds and roles
			roleCount = 0
			for role in outEvents[ev]["roles"]:
				pIdString += "," + str(outEvents[ev]["roles"][role])
				roleString += "," + role
				roleCount += 1
			# If there are less than 3 playerIds, pad the shortage with NULLs
			while roleCount < 3:
				pIdString += ",NULL"
				roleString += ",NULL"
				roleCount += 1
			# Add the playerIds and roles to the output
			outString += pIdString + roleString

		#
		# Append on-ice playerIds
		#

		# AWAY SKATERS
		pIdString = ""
		if "aSkaters" not in outEvents[ev]:
			outString += ",NULL,NULL,NULL,NULL,NULL,NULL"
		else:
			# Append playerIds
			count = 0
			for pId in outEvents[ev]["aSkaters"]:
				pIdString += "," + str(pId)
				count += 1
			# If there are less than 6 skater playerIds, pad the shortage with NULLs
			while count < 6:
				pIdString += ",NULL"
				count += 1
		outString += pIdString

		# AWAY GOALIE
		outString += "," + outputVal(outEvents[ev], "aG")

		# HOME SKATERS
		pIdString = ""
		if "hSkaters" not in outEvents[ev]:
			outString += ",NULL,NULL,NULL,NULL,NULL,NULL"
		else:
			# Append playerIds
			count = 0
			for pId in outEvents[ev]["hSkaters"]:
				pIdString += "," + str(pId)
				count += 1
			# If there are less than 6 skater playerIds, pad the shortage with NULLs
			while count < 6:
				pIdString += ",NULL"
				count += 1
		outString += pIdString

		# HOME GOALIE
		outString += "," + outputVal(outEvents[ev], "hG")

		outString += "\n"
		outFile.write(outString)

	outFile.close()

	#
	# Output team stats
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-teams.csv", "w")
	outString = "season,date,gameId,team,iceSit,strengthSit,scoreSit,"
	outString += "toi,"
	outString += "gf,ga,sf,sa,bsf,bsa,msf,msa,"
	outString += "foWon,foLost,ofo,dfo,nfo,"
	outString += "penTaken,penDrawn"
	outString += "\n"
	outFile.write(outString)

	for iceSit in outTeams:
		for strSit in strengthSits:	# Can't use "strSit in outTeam[iceSit]" because outTeams[iceSit] has additional keys: abbrev, playerIds
			for scSit in outTeams[iceSit][strSit]:
				outString = str(seasonArg)
				outString += "," + str(gameDate)
				outString += "," + str(gameId)
				outString += "," + outTeams[iceSit]["abbrev"]
				outString += "," + iceSit
				outString += "," + strSit
				outString += "," + str(scSit)

				outString += "," + str(outTeams[iceSit][strSit][scSit]["toi"])

				outString += "," + str(outTeams[iceSit][strSit][scSit]["gf"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["ga"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["sf"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["sa"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["bsf"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["bsa"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["msf"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["msa"])

				outString += "," + str(outTeams[iceSit][strSit][scSit]["foWon"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["foLost"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["ofo"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["dfo"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["nfo"])

				outString += "," + str(outTeams[iceSit][strSit][scSit]["penTaken"])
				outString += "," + str(outTeams[iceSit][strSit][scSit]["penDrawn"])

				outString += "\n"
				outFile.write(outString)

	outFile.close()

	#
	# Output player stats
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-players.csv", "w")
	outString = "season,date,gameId,team,playerId,strengthSit,scoreSit,"
	outString += "toi,ig,is,ibs,ims,ia1,ia2,blocked,"
	outString += "gf,ga,sf,sa,bsf,bsa,msf,msa,"
	outString += "foWon,foLost,ofo,dfo,nfo,"
	outString += "penTaken,penDrawn"
	outString += "\n"
	outFile.write(outString)

	for pId in outPlayers:
		for strSit in strengthSits:	# Can't use "strSit in outTeam[iceSit]" because outTeams[iceSit] has additional keys: firstname, lastname, position
			for scSit in outPlayers[pId][strSit]:
				outString = str(seasonArg)
				outString += "," + str(gameDate)
				outString += "," + str(gameId)

				# Get the player's team
				if pId in outTeams["home"]["playerIds"]:
					outString += "," + outTeams["home"]["abbrev"]
				elif pId in outTeams["away"]["playerIds"]:
					outString += "," + outTeams["away"]["abbrev"]

				outString += "," + str(pId)
				outString += "," + strSit
				outString += "," + str(scSit)

				outString += "," + str(outPlayers[pId][strSit][scSit]["toi"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ig"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["is"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ibs"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ims"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ia1"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ia2"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["blocked"])

				outString += "," + str(outPlayers[pId][strSit][scSit]["gf"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ga"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["sf"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["sa"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["bsf"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["bsa"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["msf"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["msa"])

				outString += "," + str(outPlayers[pId][strSit][scSit]["foWon"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["foLost"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["ofo"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["dfo"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["nfo"])

				outString += "," + str(outPlayers[pId][strSit][scSit]["penTaken"])
				outString += "," + str(outPlayers[pId][strSit][scSit]["penDrawn"])

				outString += "\n"
				outFile.write(outString)

	outFile.close()

	#
	# Output rosters with full player names, etc.?
	#

	outFile = open(outDir + str(seasonArg) + "-" + str(gameId) + "-rosters.csv", "w")
	outString = "season,date,gameId,team,teamIceSit,playerId,firstName,lastName,jersey,position\n"
	outFile.write(outString)

	for pId in outPlayers:

		outString = str(seasonArg)
		outString += "," + str(gameDate)
		outString += "," + str(gameId)

		# Get the player's team and iceSit
		iceSit = "NULL"
		if pId in outTeams["home"]["playerIds"]:
			outString += "," + outTeams["home"]["abbrev"]
			iceSit = "home"
		elif pId in outTeams["away"]["playerIds"]:
			outString += "," + outTeams["away"]["abbrev"]
			iceSit = "away"
		outString += "," + iceSit

		outString += "," + str(pId)
		outString += "," + outPlayers[pId]["firstName"]
		outString += "," + outPlayers[pId]["lastName"]
		outString += "," + rosters[iceSit]["players"]["ID" + str(pId)]["jerseyNumber"]
		outString += "," + outPlayers[pId]["position"]

		outString += "\n"
		outFile.write(outString)

	outFile.close()



#
# Done looping through each gameId
#