from bs4 import BeautifulSoup
from pprint import pprint
import sys
import urllib
import re
import json
import copy

#
#
# INSTRUCTIONS
#
# 1. Manually download the nhl html pbp
#		- Save it as: PL-20152016-20194.HTM
# 2. Manually retrieve the snet json
#		- Copy the value for the 'bootstrap' variable in the 3rd script element: http://www.sportsnet.ca/hockey/nhl/livetracker/game/1553527
#		- Save it as: SN-20152016-20194.json
# 3. Manually specify home and away team abbreviations using the 'teamAbbrevs' dict
# 4. Manually map player jersey numbers to playerIds using the 'playerIds' dict
#
#

season = int(sys.argv[1])	# Specify 20142015 for the 2014-2015 season
gameId = int(sys.argv[2])	# Specify a single gameId 20194

# Take string "mm:ss" and return the number of seconds (as an integer)
def toSecs(timeStr):
	mm = int(timeStr[0:timeStr.find(":")])
	ss = int(timeStr[timeStr.find(":")+1:])
	return 60 * mm + ss

#
#
# Manually specify home and away team abbreviations 
#
#

teamAbbrevs = dict()

elif season == 20152016 and gameId == 20823:
	teamAbbrevs["away"] = "stl"
	teamAbbrevs["home"] = "fla"

#
#
# Manually map team+jersey to playerIds
# Get the jersey numbers from the nhl roster report: http://www.nhl.com/scores/htmlreports/20152016/RO020194.HTM
# Get playerIds from the shift json, since it contains both names and playerIds
#

playerIds = dict()
if season == 20152016 and gameId == 20823:
	playerIds["stl-4"] = 8474125 # Gunnarsson
	playerIds["stl-6"] = 8476441 # Edmundson
	playerIds["stl-10"] = 8470105 # Upshall
	playerIds["stl-12"] = 8474627 # Lehtera
	playerIds["stl-15"] = 8477952 # Fabbri
	playerIds["stl-17"] = 8475768 # Schwartz
	playerIds["stl-19"] = 8470151 # Bouwmeester
	playerIds["stl-20"] = 8470257 # Steen
	playerIds["stl-21"] = 8473534 # Berglund
	playerIds["stl-22"] = 8474031 # Shattenkirk
	playerIds["stl-26"] = 8471669 # Stastny
	playerIds["stl-28"] = 8470803 # Brodziak
	playerIds["stl-36"] = 8471426 # Brouwer
	playerIds["stl-41"] = 8474145 # Bortuzzo
	playerIds["stl-42"] = 8470655 # Backes
	playerIds["stl-55"] = 8476892 # Parayko
	playerIds["stl-75"] = 8471817 # Reaves
	playerIds["stl-91"] = 8475765 # Tarasenko
	playerIds["stl-1"] = 8470880 # Elliott
	# playerIds["stl-30"] = Copley - no entry in shift json
	playerIds["fla-3"] = 8474000 # Kampfer
	playerIds["fla-5"] = 8477932 # Ekblad
	playerIds["fla-6"] = 8475755 # Petrovic
	playerIds["fla-7"] = 8475179 # Kulikov
	playerIds["fla-11"] = 8476456 # Huberdeau
	playerIds["fla-17"] = 8468001 # MacKenzie
	playerIds["fla-18"] = 8475191 # Smith
	playerIds["fla-21"] = 8476389 # Trocheck
	playerIds["fla-22"] = 8465978 # Thornton
	playerIds["fla-27"] = 8475760 # Bjugstad
	playerIds["fla-28"] = 8475253 # Wilson
	playerIds["fla-36"] = 8469638 # Jokinen
	playerIds["fla-37"] = 8474698 # Regner
	playerIds["fla-42"] = 8475769 # Howden
	playerIds["fla-48"] = 8476400 # Shaw
	playerIds["fla-51"] = 8466285 # Campbell
	playerIds["fla-68"] = 8448208 # Jagr
	playerIds["fla-73"] = 8475204 # Pirri
	playerIds["fla-1"] = 8466141 # Luongo
	playerIds["fla-35"] = 8471219 # Montoya

#
#
# Load html pbp file
#
#

inDir = "fallback-data/raw/"
inFilename = "PL-" + str(season) + "-" + str(gameId) + ".HTM"
htmlInFile = file(inDir + inFilename, "r")
soup = BeautifulSoup(htmlInFile.read(), "lxml")
htmlInFile.close()

#
# Loop through each html play and prepare it for output
# The output file will be used by scrape-game.py
#

outEvents = []

rows = soup.find_all("tr", class_="evenColor")
for r in rows:

	pDict = dict()

	# Get eventId and description
	pDict["id"] = int(r.find_all("td", class_=re.compile("bborder"))[0].text)
	evDesc = r.find_all("td", class_=re.compile("bborder"))[5].text
	evDesc = evDesc.replace(unichr(160), " ") # Replace non-breaking spaces with spaces
	pDict["description"] = evDesc

	# Get event type and convert it to the those used in the json pbp
	# We won't worry about getting the subtypes
	pDict["type"] = r.find_all("td", class_=re.compile("bborder"))[4].text.lower()

	# Update the html event types to the ones used in the json pbp
	if pDict["type"] == "pstr":
		pDict["type"] = "period_start"
	elif pDict["type"] == "pend":
		pDict["type"] = "period_end"
	elif pDict["type"] == "gend":
		pDict["type"] = "game_end"
	elif pDict["type"] == "fac":
		pDict["type"] = "faceoff"
	elif pDict["type"] == "miss":
		pDict["type"] = "missed_shot"
	elif pDict["type"] == "block":
		pDict["type"] = "blocked_shot"
	elif pDict["type"] == "give":
		pDict["type"] = "giveaway"
	elif pDict["type"] == "take":
		pDict["type"] = "takeaway"
	elif pDict["type"] == "penl":
		pDict["type"] = "penalty"

	# Get event subtype
	if pDict["type"] in ["goal", "missed_shot", "blocked_shot", "shot"]:
		if evDesc.lower().find(", slap,") >= 0:
			pDict["subtype"] = "slap shot"
		elif evDesc.lower().find(", snap,") >= 0:
			pDict["subtype"] = "snap shot"
		elif evDesc.lower().find(", wrist,") >= 0:
			pDict["subtype"] = "wrist shot"
		elif evDesc.lower().find(", deflected,") >= 0:
			pDict["subtype"] = "deflection"
		elif evDesc.lower().find(", backhand,") >= 0:
			pDict["subtype"] = "backhand"
		elif evDesc.lower().find(", tip-in,") >= 0:
			pDict["subtype"] = "tip-in"
		elif evDesc.lower().find(", wrap-around,") >= 0:
			pDict["subtype"] = "wrap-around"
	elif pDict["type"] == "penalty":
		# Identify penalties that resulted in a penalty shot
		subtypeStart = re.search("[a-z]", evDesc).start() # We can find where the penalty type starts by finding the first lowercase letter in the description
		pDict["subtype"] = evDesc[subtypeStart - 1:evDesc.find("(")]
		pDict["penMins"] = int(evDesc[evDesc.find("(") + 1:evDesc.find("min)")].replace(" ", ""))
		if pDict["penMins"] == 0:
			pDict["penSeverity"] = "penalty shot"
		elif pDict["penMins"] == 2:
			if evDesc.find(" bench(") >= 0:
				pDict["penSeverity"] = "bench minor"
			else:
				pDict["penSeverity"] = "minor"
		elif pDict["penMins"] == 4:
			pDict["penSeverity"] = "minor" # in the json pbp, 'double minor' is included in the description and the severity is listed as 'minor'
		elif pDict["penMins"] == 5:
			pDict["penSeverity"] = "major"
		elif pDict["penMins"] == 10:
			if evDesc.find("match penalty") >= 0:
				pDict["penSeverity"] = "match penalty"
			elif evDesc.find("misconduct") >= 0:
				pDict["penSeverity"] = "misconduct"

	# Periods in the html pbp are always numbered 1, 2, 3, 4, 5 (in regular season, period 5 is the SO)
	pDict["period"] = int(r.find_all("td", class_=re.compile("bborder"))[1].text)
	if pDict["period"] <= 3:
		pDict["periodType"] = "regular"
	elif gameId < 30000 and pDict["period"] == 4:
		pDict["periodType"] = "overtime"
	elif gameId < 30000 and pDict["period"] == 5:
		pDict["periodType"] = "shootout"
	elif gameId >= 30000:
		pDict["periodType"] = "overtime"

	# Convert elapsed time to seconds
	timeRange = r.find_all("td", class_=re.compile("bborder"))[3]
	timeElapsed = timeRange.find("br").previousSibling
	pDict["time"] = toSecs(timeElapsed)

	# Get the team that TOOK the shot, MADE the hit, COMMITTED the penalty, WON the faceoff, etc.
	# Update the html team abbreviations to the ones used in the nhl json pbp
	evTeam = evDesc[0:evDesc.find(" ")].lower()
	if evTeam == "n.j":
		evTeam = "njd"
	elif evTeam == "s.j":
		evTeam = "sjs"
	elif evTeam == "t.b":
		evTeam = "tbl"
	elif evTeam == "l.a":
		evTeam = "lak"

	if evTeam in [teamAbbrevs["away"], teamAbbrevs["home"]]:
		pDict["team"] = evTeam

		# Get the iceSit for the event team
		if pDict["team"] == teamAbbrevs["away"]:
			pDict["iceSit"] = "away"
		elif pDict["team"] == teamAbbrevs["home"]:
			pDict["iceSit"] = "home"
	else:
		evTeam = None

	#
	# Parse the event description to produce the same roles found in the json
	#

	rolesDict = dict()

	if pDict["type"] == "faceoff":
		aTaker = evDesc.split("#")[1]				# The away FO taker is always listed first
		aTaker = aTaker[0:aTaker.find(" ")]
		hTaker = evDesc.split("#")[2]				# The home FO taker is always listed first
		hTaker = hTaker[0:hTaker.find(" ")]

		if pDict["team"] == teamAbbrevs["away"]:
			rolesDict["winner"] = teamAbbrevs["away"] + "-" + aTaker
			rolesDict["loser"] = teamAbbrevs["home"] + "-" + hTaker
		elif pDict["team"] == teamAbbrevs["home"]:
			rolesDict["winner"] = teamAbbrevs["home"] + "-" + hTaker
			rolesDict["loser"] = teamAbbrevs["away"] + "-" + aTaker

	elif pDict["type"] in ["shot", "missed_shot"]:
		
		shooter = evDesc.split("#")[1]				# Only a single player is listed for shots-on-goal and misses
		shooter = shooter[0:shooter.find(" ")]
		rolesDict["shooter"] = pDict["team"] + "-" + shooter

	elif pDict["type"] == "blocked_shot":
		
		shooter = evDesc.split("#")[1]				# The shooter is always listed first
		shooter = shooter[0:shooter.find(" ")]
		blocker = evDesc.split("#")[2]				# The blocker is always listed first
		blocker = blocker[0:blocker.find(" ")]

		rolesDict["shooter"] = pDict["team"] + "-" + shooter
		if pDict["team"] == teamAbbrevs["away"]:
			rolesDict["blocker"] = teamAbbrevs["home"] + "-" + blocker
		elif pDict["team"] == teamAbbrevs["home"]:
			rolesDict["blocker"] = teamAbbrevs["away"] + "-" + blocker

	elif pDict["type"] in ["giveaway", "takeaway"]:

		player = evDesc.split("#")[1]				# Only a single player is listed for giveaways and takeaway
		player = player[0:player.find(" ")]
		player = pDict["team"] + "-" + player

		if pDict["type"] == "give":
			rolesDict["giver"] = player
		elif pDict["type"] == "take":
			rolesDict["taker"] = player

	elif pDict["type"] == "goal":

		numPlayers = evDesc.count("#")
		if numPlayers >= 1:
			scorer = evDesc.split("#")[1]		# Scorer is always listed first
			scorer = scorer[0:scorer.find(" ")]
			rolesDict["scorer"] = pDict["team"] + "-" + scorer
		if numPlayers >= 2:
			a1 = evDesc.split("#")[2]			# Primary assister is always listed second
			a1 = a1[0:a1.find(" ")]
			rolesDict["assist1"] = pDict["team"] + "-" + a1
		if numPlayers >= 3:
			a2 = evDesc.split("#")[3]			# Secondary assister is always listed second
			a2 = a2[0:a2.find(" ")]
			rolesDict["assist2"] = pDict["team"] + "-" + a2

	elif pDict["type"] == "hit":

		hitter = evDesc.split("#")[1]			# Hitter is always listed first
		hitter = hitter[0:hitter.find(" ")]			
		hittee = evDesc.split("#")[2]
		hittee = hittee[0:hittee.find(" ")]

		rolesDict["hitter"] = pDict["team"] + "-" + hitter
		if pDict["team"] == teamAbbrevs["away"]:
			rolesDict["hittee"] = teamAbbrevs["home"] + "-" + hittee
		elif pDict["team"] == teamAbbrevs["home"]:
			rolesDict["hittee"] = teamAbbrevs["away"] + "-" + hittee

	elif pDict["type"] == "penalty":

		# Get the content between the 1st and 2nd spaces
		# If a player took the penalty, then it will return #XX
		# If a team took the penalty, then it will return 'TEAM'
		penaltyOn = evDesc.split(" ")[1]
		poundIdx = penaltyOn.find("#")
		if poundIdx >= 0:
			penaltyOn = penaltyOn[poundIdx+1:]
			rolesDict["penaltyon"] = pDict["team"] + "-" + penaltyOn

		# Get the player who drew the penalty
		drawnBy = None
		pattern = "Drawn By: "
		drawnByIdx = evDesc.find(pattern)
		if drawnByIdx >= 0:								# Only search for the pattern if it exists
			drawnBy = evDesc[evDesc.find(pattern):]		# Returns a substring *starting* with the pattern
			drawnBy = drawnBy[len(pattern):]			# Remove the pattern from the substring
			drawnBy = drawnBy[drawnBy.find("#")+1:]		# Remove the team abbreviation and "#" from the beginning of the string											
			drawnBy = drawnBy[0:drawnBy.find(" ")]		# Isolate the jersey number

			if pDict["team"] == teamAbbrevs["away"]:	# The penalty-drawer is always on the opposite team of the penalty-taker
				rolesDict["drewby"] = teamAbbrevs["home"] + "-" + drawnBy
			elif pDict["team"] == teamAbbrevs["home"]:
				rolesDict["drewby"] = teamAbbrevs["away"] + "-" + drawnBy

		# Get the player who served the penalty
		servedBy = None
		pattern = "Served By: #"
		servedByIdx = evDesc.find(pattern)
		if servedByIdx >= 0:							# Only search for the pattern if it exists
			servedBy = evDesc[evDesc.find(pattern):]	# Returns a substring *starting* with the pattern
			servedBy = servedBy[len(pattern):]			# Remove the pattern from the substring
			servedBy = servedBy[0:servedBy.find(" ")]	# Isolate the jersey number
			rolesDict["servedby"] = pDict["team"] + "-" + servedBy

	#
	# Convert jersey numbers into playerIds and store the dict
	#

	if len(rolesDict) > 0:
		for role in rolesDict:
			rolesDict[role] = playerIds[rolesDict[role]]
		pDict["roles"] = copy.deepcopy(rolesDict)

	#
	# Get the zone in which the event occurred - always use the home team's perspective
	#

	if "team" in pDict:
		if pDict["type"] == "blocked_shot":
			if pDict["team"] == teamAbbrevs["home"] and evDesc.lower().find("off. zone") >= 0:		# home team took shot, blocked by away team in away team's off. zone
				pDict["hZone"] = "d"
			elif pDict["team"] == teamAbbrevs["away"] and evDesc.lower().find("def. zone") >= 0:	# away team took shot, blocked by home team in home team's def. zone
				pDict["hZone"] = "d"
			elif pDict["team"] == teamAbbrevs["home"] and evDesc.lower().find("def. zone") >= 0:	# home team took shot, blocked by away team in away team's def. zone
				pDict["hZone"] = "o"
			elif pDict["team"] == teamAbbrevs["away"] and evDesc.lower().find("off. zone") >= 0:	# away team took shot, blocked by home team in home team's off. zone
				pDict["hZone"] = "o"
			elif evDesc.lower().find("neu. zone") >= 0:
				pDict["hZone"] = "n"
		else: 
			if pDict["team"] == teamAbbrevs["home"] and evDesc.lower().find("off. zone") >= 0:		# home team created event (excluding blocked shot) in home team's off. zone (incl. winning face off)
				pDict["hZone"] = "o"
			elif pDict["team"] == teamAbbrevs["away"] and evDesc.lower().find("def. zone") >= 0:
				pDict["hZone"] = "o"
			elif pDict["team"] == teamAbbrevs["home"] and evDesc.lower().find("def. zone") >= 0:
				pDict["hZone"] = "d"
			elif pDict["team"] == teamAbbrevs["away"] and evDesc.lower().find("off. zone") >= 0:
				pDict["hZone"] = "d"
			elif evDesc.lower().find("neu. zone") >= 0:
				pDict["hZone"] = "n"

	# Create a flag to prevent the event from being matched to multiple snet json events
	pDict["matched"] = False

	outEvents.append(copy.deepcopy(pDict))

#
#
# Append the home and away scores to each event
#
#

# Initialize the home and away score for each event
for ev in outEvents:
	ev["aScore"] = 0
	ev["hScore"] = 0

# Increment the home and away score after each non-shootout goal
for ev in outEvents:
	if ev["type"] == "goal" and ev["periodType"] != "shootout":

		# Look for events that occurred after (or at the same time) as the goal
		for ev1 in outEvents:
			if ev1["period"] > ev["period"] or (ev1["period"] == ev["period"] and ev1["time"] > ev["time"]):
				# For events that occur after the goal, increment the goal counts
				if ev["iceSit"] == "away":
					ev1["aScore"] += 1
				elif ev["iceSit"] == "home":
					ev1["hScore"] += 1
			elif ev1["period"] == ev["period"] and ev1["time"] == ev["time"]:
				# For events that have the same time as the goal, only increment the goal counts for the post-goal faceoff
				if ev1["type"] == "faceoff":
					if ev["iceSit"] == "away":
						ev1["aScore"] += 1
					elif ev["iceSit"] == "home":
						ev1["hScore"] += 1

#
#
# Load the snet file to get locations for: goals, shots, misses, blocks, penalties, hits
# Other event types aren't included in the snet data
#
#

#
# Get the list of play and team objects from the snet json
#

inFilename = "SN-" + str(season) + "-" + str(gameId) + ".json"
jsonInFile = file(inDir + inFilename, "r")
jsonDict = json.loads(jsonInFile.read())
jsonInFile.close()

snetEvs = dict()
snetTeams = dict()
snetPlayers = dict()
gameDate = None
numPeriods = None

for key, value in jsonDict.items():
	if key == "plays":
		snetEvs = value
	elif key == "players":
		snetPlayers = value
	elif key == "league":
		# Loop through each game listed in the 'league' object and find the matching game
		for game in value: 
			if int(game["id"]) == gameId:
				snetTeams = game["team"]
				numPeriods = game["period"]
	elif key == "game":
		gameDate = value["startTime"]

del jsonDict

#
# Map snet teamIds to team abbreviations
#

snetTeamAbbrevs = dict()
snetFinalScores = dict()
for team in snetTeams:
	snetTeamAbbrevs[team["id"]] = team["abbr"].lower()
	snetFinalScores[team["alignment"]] = team["score"]

#
# Update snet events to use the same values as the html so that we can match events from snet and nhl pbps
#

for ev in snetEvs:

	#
	# Translate event types
	#

	if ev["event"] == "score":
		ev["event"] = "goal"
	elif ev["event"] == "shot-on-goal":
		ev["event"] = "shot"
	elif ev["event"] == "shot-missed":
		ev["event"] = "missed_shot"
	elif ev["event"] == "shot-blocked":
		ev["event"] = "blocked_shot"

	#
	# Translate roles
	#

	if "participants" in ev:

		if ev["event"] == "goal":
			assistCount = 0
			for party in ev["participants"]:
				if party["role"] == "assist":
					assistCount += 1
					party["role"] = "assist" + str(assistCount)
				elif party["role"] == "goaltender":
					party["role"] = "goalie"
		elif ev["event"] == "shot":
			for party in ev["participants"]:
				if party["role"] == "goaltender":
					party["role"] = "goalie"
		elif ev["event"] == "penalty":
			for party in ev["participants"]:
				if party["role"] == "penalty-committed-by":
					party["role"] = "penaltyon"
				elif party["role"] == "penalty-committed-against":
					party["role"] = "drewby"
				elif party["role"] == "penalty-served-by":
					party["role"] = "servedby"

	#
	# Convert roles to dictionary so we can compare it to the html events' role dictionary
	#

	if "participants" in ev:
		ev["roles"] = dict()		# This matches the nhl's html pbp: goalies aren't listed for any shots or goals
		ev["fullRoles"] = dict()	# This matches the nhl's json pbp: goalies are only listed for saved shots (not goals)

		for party in ev["participants"]:
			if party["playerId"] is not None:	# Some roles in the json have playerId: None - skip these
				if party["role"] == "goalie":
					if ev["type"] == "shot":
						ev["fullRoles"][party["role"]] = party["playerId"]
				else:
					ev["roles"][party["role"]] = party["playerId"]
					ev["fullRoles"][party["role"]] = party["playerId"]

		del ev["participants"]

	# Convert event time
	ev["time"] = toSecs(ev["elapsed"])

	# Get team abbreviation
	# For blocked shots, the snet json attributes the event to the blocker's team - flip this
	ev["team"] = snetTeamAbbrevs[ev["teamId"]]
	del ev["teamId"]
	if ev["event"] == "blocked_shot":
		if ev["team"] == teamAbbrevs["home"]:
			ev["team"] = teamAbbrevs["away"]
		elif ev["team"] == teamAbbrevs["away"]:
			ev["team"] = teamAbbrevs["home"]

	# Create a flag to prevent the json event from being matched to multiple html events
	ev["matched"] = False

#
#
# Add json event location data to html events
# Also add the fullRoles from the snet json that contains the goalie on saved shots (to be consistent with the nhl json)
#
#

for ev in outEvents:

	if ev["type"] in ["goal", "shot", "missed_shot", "blocked_shot", "penalty", "hit"]:

		for jEv in snetEvs:

			if (ev["matched"] == False and jEv["matched"] == False
				and ev["period"] == jEv["period"] and ev["time"] == jEv["time"]
				and ev["team"] == jEv["team"] and ev["roles"] == jEv["roles"]
				and ev["type"] == jEv["event"]):

				jEv["matched"] = True
				ev["matched"] = True

				ev["roles"] = jEv["fullRoles"]
				ev["coords"] = jEv["location"]

#
# For validation, print any unmatched json events
#

for jEv in snetEvs:
	if jEv["matched"] == False:
		print "Unmatched snet json event:"
		pprint(jEv)

#
#
# Clean up outEvents for output
#
#

for ev in outEvents:

	# Delete matched flag
	del ev["matched"]

	# Split up coordinates
	if "coords" in ev:
		ev["locX"] = ev["coords"][0]
		ev["locY"] = ev["coords"][1]
		del ev["coords"]

#
#
# Prepare team data for output
#
#

outTeams = dict()
outTeams["away"] = dict()
outTeams["away"]["abbreviation"] = teamAbbrevs["away"]
outTeams["home"] = dict()
outTeams["home"]["abbreviation"] = teamAbbrevs["home"]

#
#
# Prepare player data for output
#
#

outPlayers = dict()

for player in playerIds:

	# Create a dictionary for each playerId to store player information
	# To match the nhl json, use "ID#" as the player key
	pKey = "ID" + str(playerIds[player])
	outPlayers[pKey] = dict()
	outPlayers[pKey]["id"] = playerIds[player]

	# Get jersey number
	outPlayers[pKey]["jersey"] = int(player[player.find("-") + 1:])

	# Get team abbreviation
	outPlayers[pKey]["team"] = player[0:player.find("-")]

	# Get iceSit
	if outPlayers[pKey]["team"] == teamAbbrevs["home"]:
		outPlayers[pKey]["iceSit"] = "home"
	elif outPlayers[pKey]["team"] == teamAbbrevs["away"]:
		outPlayers[pKey]["iceSit"] = "away"

#
# Append additional player information from the snet data
#

for pKey in outPlayers:
	for snetPlayer in snetPlayers:
		if outPlayers[pKey]["id"] == snetPlayer["id"]:
			outPlayers[pKey]["firstName"] = snetPlayer["firstName"]
			outPlayers[pKey]["lastName"] = snetPlayer["lastName"]
			outPlayers[pKey]["primaryPosition"] = dict()
			outPlayers[pKey]["primaryPosition"]["abbreviation"] = snetPlayer["positionAbbr"].lower()
			if outPlayers[pKey]["primaryPosition"]["abbreviation"] == "l":
				outPlayers[pKey]["primaryPosition"]["abbreviation"] = "lw"
			elif outPlayers[pKey]["primaryPosition"]["abbreviation"] == "r":
				outPlayers[pKey]["primaryPosition"]["abbreviation"] = "rw"

#
#
# Output event, player, and team data as json
# In most cases, we'll try to replicate the structure of the nhl json
#
#

outDict = dict()

# Game date
outDict["gameData"] = dict()
outDict["gameData"]["datetime"] = dict()
outDict["gameData"]["datetime"]["dateTime"] = gameDate

# Team and player data
outDict["gameData"]["teams"] = outTeams
outDict["gameData"]["players"] = outPlayers

# Event data
outDict["liveData"] = dict()
outDict["liveData"]["plays"] = dict()
outDict["liveData"]["plays"]["allPlays"] = outEvents

# Output game results
# Hardcode the 'time remaining' value since we're working with finished games
outDict["liveData"]["linescore"] = dict()
outDict["liveData"]["linescore"]["currentPeriodTimeRemaining"] = "final"
outDict["liveData"]["linescore"]["currentPeriod"] = numPeriods


lastPeriodName = "unknown"
if numPeriods == 1:
	lastPeriodName = "1st"
elif numPeriods == 2:
	lastPeriodName = "2nd"
elif numPeriods == 3:
	lastPeriodName = "3rd"
elif gameId < 30000:	# Regular season overtime and shootout
	if numPeriods == 4:
		lastPeriodName = "ot"
	elif numPeriods == 5:
		lastPeriodName = "so"
elif gameId >= 30000:	# Playoff overtimes
	lastOtPeriod = numPeriods - 3
	lastPeriodName = "ot" + st(lastOtPeriod)
outDict["liveData"]["linescore"]["currentPeriodOrdinal"] = lastPeriodName

outDict["liveData"]["linescore"]["teams"] = dict()
outDict["liveData"]["linescore"]["teams"]["away"] = dict()
outDict["liveData"]["linescore"]["teams"]["home"] = dict()
outDict["liveData"]["linescore"]["teams"]["away"]["goals"] = snetFinalScores["away"]
outDict["liveData"]["linescore"]["teams"]["home"]["goals"] = snetFinalScores["home"]

# Write output to file
outDir = "fallback-data/processed/"
outFilename = outDir + "PL-" + str(season) + "-" + str(gameId) + "-processed.json"	
outFile = open(outFilename, "w")
outFile.write(json.dumps(outDict).encode("utf-8"))
outFile.close()
		
