from bs4 import BeautifulSoup
from pprint import pprint
import urllib
import os.path
import sys
import mysql.connector
import re
import dbconfig	# Database credentials

#
# 
# Get arguments
#
#

seasonArg = int(sys.argv[1])	# Specify 20142015 for the 2014-2015 season
gameArg = str(sys.argv[2])		# Specify a gameId 20100, or a range 20100-20105

# Convert gameArg into a list of gameIds
gameIds = []
if gameArg.find("-") > 0:
	startId = int(gameArg[0:gameArg.find("-")])
	endId = int(gameArg[gameArg.find("-") + 1:])
	for gameId in range(startId, endId + 1):
		gameIds.append(int(gameId))
else:
	gameIds = [int(gameArg)]

#
#
# Configuration
#
#

# Database connection
connection = mysql.connector.connect(user=dbconfig.user, passwd=dbconfig.passwd, host=dbconfig.host, database=dbconfig.database)
cursor = connection.cursor()

# Directories
inDir = "nhl-data/" + str(seasonArg) + "/"
outDir = "data-for-db/" + str(seasonArg) + "/"

#
#
# Scrape data for each game
#
#

for gameId in gameIds:

	teams = dict()
	teams["away"] = ""
	teams["home"] = ""

	periodDurations = dict()

	#
	#
	# Load the PBP html file
	#
	#

	# Download the PBP html file if it doesn't already exist
	filename = "PL0" + str(gameId) + ".HTM"
	if os.path.isfile(inDir + filename) == False:
		print "Downloading " + str(filename)
		nhlUrl = "http://www.nhl.com/scores/htmlreports/" + str(seasonArg) + "/PL0" + str(gameId) + ".HTM"
		user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.94 Safari/537.36"
		headers = { "User-Agent" : user_agent }
		savedFile = urllib.urlretrieve(nhlUrl, inDir + "PL0" + str(gameId) + ".HTM")
		
	print "Getting team abbreviations from " + str(filename)

	# Load PBP html file
	htmlFile = file(inDir + "PL0" + str(gameId) + ".HTM", "r")
	soup = BeautifulSoup(htmlFile.read(), "lxml")
	htmlFile.close()

	#
	#
	# Store home and away team name abbreviations
	# These will be reused to produce the player and shift output files
	#
	#

	els = soup.find_all("td", class_="heading + bborder", width="10%", align="center")
	awayTeam = els[0].text.lower()
	teams["away"] = awayTeam[0:awayTeam.find(" ")]
	homeTeam = els[1].text.lower()
	teams["home"] = homeTeam[0:homeTeam.find(" ")]
	
	#
	#
	# Get the game date from the PBP html file, and store it as an int (20150123)
	# This will be reused to produce the player and shift output files
	#
	#

	gameInfoTable = soup.find_all("table", id="GameInfo")[0]
	date = gameInfoTable.find_all("tr")[3].text
	date = date.replace(",", ";")
	date = date.strip("\r\n")		# Strip new line and spaces from ends of string
	date = date[date.find("; ")+2:] # Remove the day (Monday, Tuesday, etc.) from the string

	yyyy = int(date[date.find("; ")+2:])
	mm = 0
	month = date[0:date.find(" ")].lower()
	if month == "january":
		mm = 1
	elif month == "february":
		mm = 2
	elif month == "march":
		mm = 3
	elif month == "april":
		mm = 4
	elif month == "may":
		mm = 5
	elif month == "june":
		mm = 6
	elif month == "july":
		mm = 7
	elif month == "august":
		mm = 8
	elif month == "september":
		mm = 9
	elif month == "october":
		mm = 10
	elif month == "november":
		mm = 11
	elif month == "december":
		mm = 12
	dd = int(date[date.find(" ")+1:date.find("; ")])
	date = 10000*yyyy + 100*mm + dd

	#
	#
	# Process player attributes from the boxscore
	# Do this before parsing the PBP and shift files so that we can use unique playerIds instead of jersey numbers
	#
	#

	boxscoreUrl = str(seasonArg)[0:4] + "0" + str(gameId)

	# If boxscore html file doesn't exist, download it
	filename = "Box0" + str(gameId) + ".html"
	if os.path.isfile(inDir + filename) == False:
		print "Downloading " + str(filename)
		nhlUrl = "http://www.nhl.com/gamecenter/en/boxscore?id=" + boxscoreUrl
		user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.94 Safari/537.36"
		headers = { "User-Agent" : user_agent }
		savedFileName = "Box0" + str(gameId) + ".html"
		savedFile = urllib.urlretrieve(nhlUrl, inDir + savedFileName)
		
	print "Processing " + str(filename)
	
	# Load boxscore html file
	htmlFile = file(inDir + "Box0" + str(gameId) + ".html", "r")
	soup = BeautifulSoup(htmlFile.read(), "lxml")
	htmlFile.close()

	# Create dictionaries for the home and away players
	# The keys will be the jersey numbers, so that when we process the PBP and shift files, we can translate the jersey numbers into playerIds
	players = dict()
	players["away"] = dict()
	players["home"] = dict()
	
	# Get all the stat tables - includes both teams' skaters and goalies
	tables = soup.find_all("table", class_="stats")
	for i, table in enumerate(tables):

		# The first and second tables are for the away team; third and fourth tables are for the home team
		team = ""
		if i == 0 or i == 1:
			team = "away"
		elif i == 2 or i == 3:
			team = "home"

		# Loop through each row (a player) of the current stats table
		# Create a dictionary entry for each player (using their jersey number as the key)
		rows = table.find_all("tr", class_="statsValues")
		for r in rows:

			# The first table is the away skaters, third table is the home skaters
			if i == 0 or i == 2:

				jersey = int(r.find_all("td", colspan="1", rowspan="1")[0].text)
				players[team][jersey] = dict()

				players[team][jersey]["name"] = r.find_all("a", class_="undMe", rel="skaterLinkData")[0].text
				players[team][jersey]["position"] = r.find_all("td", colspan="1", rowspan="1")[2].text.lower()

				href = r.find_all("a", class_="undMe", rel="skaterLinkData")[0]["href"]
				players[team][jersey]["playerId"] = href[href.find("?id=")+4:]

			# The second table is the away goalies, fourth table is the home goalies
			if i == 1 or i == 3:

				jersey = int(r.find_all("td", colspan="1", rowspan="1")[0].text)
				players[team][jersey] = dict()

				players[team][jersey]["name"] = r.find_all("a", class_="undMe", rel="goalieLinkData")[0].text
				players[team][jersey]["position"] = "g"

				href = r.find_all("a", class_="undMe", rel="goalieLinkData")[0]["href"]
				players[team][jersey]["playerId"] = href[href.find("?id=")+4:]

	# Done looping through stats tables

	# 
	#
	# Record each event in the PBP html file
	#
	#
	
	print "Processing " + str("PL0" + str(gameId) + ".HTM")
	htmlFile = file(inDir + "PL0" + str(gameId) + ".HTM", "r")
	soup = BeautifulSoup(htmlFile.read(), "lxml")
	htmlFile.close()

	# Record the score at which event occurred
	awayScore = 0
	homeScore = 0

	# Loop through each play in the HTML file
	events = dict()
	rows = soup.find_all("tr", class_="evenColor")
	for r in rows:

		# Create a dictionary for each event
		eventId = int(r.find_all("td", class_=re.compile("bborder"))[0].text)
		events[eventId] = dict()
		events[eventId]["eventId"] = eventId

		# Periods in the NHL play-by-play data are always numbered 1, 2, 3, 4, 5 (in regular season, period 5 is the SO)
		# Period numbering in the shift data is different, though - see shift-processing code for example OT/SO games
		events[eventId]["period"] = int(r.find_all("td", class_=re.compile("bborder"))[1].text)
		events[eventId]["type"] = r.find_all("td", class_=re.compile("bborder"))[4].text.lower()
		desc = (r.find_all("td", class_=re.compile("bborder"))[5].text).replace(",", ";")
		events[eventId]["desc"] = desc.replace(unichr(160), " ") # Replace non-breaking spaces with spaces
		events[eventId]["gameId"] = gameId
		events[eventId]["season"] = seasonArg
		events[eventId]["date"] = date

		# Convert elapsed time to seconds
		timeRange = r.find_all("td", class_=re.compile("bborder"))[3]
		timeElapsed = timeRange.find("br").previousSibling
		elapsedMin = int(timeElapsed[0:timeElapsed.find(":")])
		elapsedSec = int(timeElapsed[timeElapsed.find(":")+1:])
		events[eventId]["time"] = 60 * elapsedMin + elapsedSec

		# Record the duration of each period
		if events[eventId]["type"] == "pend":
			periodDurations[events[eventId]["period"]] = events[eventId]["time"]

		#
		#
		# Parse the description text
		#
		#

		# Get the team that took the shot, made the hit, or won the faceoff, etc.
		eventTeam = events[eventId]["desc"][0:events[eventId]["desc"].find(" ")].lower()
		if eventTeam not in [teams["away"], teams["home"]]:
			eventTeam = ""
		events[eventId]["team"] = eventTeam

		# P1, P2, P3 are the players that are listed in the PBP data (in the order that they're listed)
 		# If the event is a FAC: P1 won, P2 lost
 		# If the event is a PENL: P1 committed the penalty, P2 drew it
 		# If the event is a HIT: P1 is the hitter, P2 was hit
 		# If the event is a GIVE: P1 gave it away
 		# If the event is a TAKE: P1 took it away
 		# If the event is a GOAL: P1 is the scorer, P2&P3 assisted
 		# If the event is a SHOT: P1 shot it
 		# If the event is a MISS: P1 shot it
 		# If the event is a BLOCK: P1 shot it, P2 blocked it
 		eventP1 = "NULL"
 		eventP2 = "NULL"
 		eventP3 = "NULL"
		numPlayers = events[eventId]["desc"].count("#")
		if numPlayers >= 1:
			eventP1 = events[eventId]["desc"].split("#")[1]
			eventP1 = int(eventP1[0:eventP1.find(" ")])
		if numPlayers >= 2:
			eventP2 = events[eventId]["desc"].split("#")[2]
			eventP2 = int(eventP2[0:eventP2.find(" ")])
		if numPlayers == 3:
			eventP3 = events[eventId]["desc"].rsplit("#")[3]
			eventP3 = int(eventP3[0:eventP3.find(" ")])

		# For face-offs, the PBP file always lists the away player first, home player second
		# But we want the winner to be eventP1 and the loser to be eventP2, so switch eventP1 and eventP2 if the homeTeam won the faceoff
		if events[eventId]["type"] == "fac" and events[eventId]["team"] == teams["home"]:
			tempP = eventP1
			eventP1 = eventP2
			eventP2 = tempP

		# Convert jersey numbers into playerIds using the 'players' dictionary
		# Depending on the event type, we need to look up the jersey number in the home or away player dictionaries
		if numPlayers >= 1:
			# If only a single jersey number exists, the listed played usually belongs to the eventTeam
			# Exception 1:
			#	"S.J TEAM Player leaves bench - bench(2 min), Off. Zone Drawn By: ANA #47 LINDHOLM" - no SJ player is listed
			#	See event #341 here: http://www.nhl.com/scores/htmlreports/20142015/PL020120.HTM
			if events[eventId]["type"] == "penl" and events[eventId]["desc"].lower().find("player leaves bench") >= 0:
				if events[eventId]["team"] == teams["away"]:
					eventP1 = players["home"][eventP1]["playerId"]
				elif events[eventId]["team"] == teams["home"]:
					eventP1 = players["away"][eventP1]["playerId"]
			else:
				if events[eventId]["team"] == teams["away"]:
					eventP1 = players["away"][eventP1]["playerId"]
				elif events[eventId]["team"] == teams["home"]:
					eventP1 = players["home"][eventP1]["playerId"]

		if numPlayers >= 2:	
			if (events[eventId]["type"] in ["fac", "hit", "block"]) or (events[eventId]["type"] == "penl" and events[eventId]["desc"].lower().find(" served by: ") < 0) or (events[eventId]["type"] == "penl" and events[eventId]["desc"].lower().find("too many men/ice") >= 0):
			# For these events, eventP2 is eventP1's opponent, so we use the opposite player dictionary.
			# Don't use the opposite dictionary if the penalty description contains "served by" - in this case, the player in the box is on the same team as eventP1. 
			# This case also includes "too many men" bench penalties because P1 is the serving player, P2 is the drawing player: "COL TEAM Too many men/ice - bench(2 min) Served By: #28 CAREY, Neu. Zone Drawn By: NSH #20 VOLCHENKOV"
				if events[eventId]["team"] == teams["away"]:
					eventP2 = players["home"][eventP2]["playerId"]
				elif events[eventId]["team"] == teams["home"]:
					eventP2 = players["away"][eventP2]["playerId"]
			elif events[eventId]["type"] == "goal" or (events[eventId]["type"] == "penl" and events[eventId]["desc"].lower().find(" served by: ") >= 0): 
				if events[eventId]["team"] == teams["away"]:
					eventP2 = players["away"][eventP2]["playerId"]
				elif events[eventId]["team"] == teams["home"]:
					eventP2 = players["home"][eventP2]["playerId"]

		if numPlayers == 3:
			# 3 players are only listed in goals with 2 assists, and for penalties that were served by someone other than the committer
			if events[eventId]["type"] == "goal":
				if events[eventId]["team"] == teams["away"]:
					eventP3 = players["away"][eventP3]["playerId"]
				elif events[eventId]["team"] == teams["home"]:
					eventP3 = players["home"][eventP3]["playerId"]
			elif events[eventId]["type"] == "penl" and events[eventId]["desc"].lower().find(" served by: ") >= 0: 
				if events[eventId]["team"] == teams["away"]:
					eventP3 = players["home"][eventP3]["playerId"]
				elif events[eventId]["team"] == teams["home"]:
					eventP3 = players["away"][eventP3]["playerId"]

		# Store the eventPlayerIds
 		events[eventId]["p1"] = eventP1
 		events[eventId]["p2"] = eventP2
 		events[eventId]["p3"] = eventP3

 		# Store the shot type or penalty type
		if events[eventId]["type"] in ["goal", "shot", "block", "miss"]:
			if events[eventId]["desc"].lower().find("; slap;") >= 0:
				events[eventId]["subtype"] = "slap"
			elif events[eventId]["desc"].lower().find("; snap;") >= 0:
				events[eventId]["subtype"] = "snap"
			elif events[eventId]["desc"].lower().find("; wrist;") >= 0:
				events[eventId]["subtype"] = "wrist"
			elif events[eventId]["desc"].lower().find("; deflected;") >= 0:
				events[eventId]["subtype"] = "deflected"
			elif events[eventId]["desc"].lower().find("; backhand;") >= 0:
				events[eventId]["subtype"] = "backhand"
			elif events[eventId]["desc"].lower().find("; tip-in;") >= 0:
				events[eventId]["subtype"] = "tip-in"
			elif events[eventId]["desc"].lower().find("; wrap-around;") >= 0:
				events[eventId]["subtype"] = "wrap-around"
		elif events[eventId]["type"] == "penl":
			subtypeStart = re.search("[a-z]", events[eventId]["desc"]).start() # We can find where the penalty type starts by finding the first lowercase letter in the description
			events[eventId]["subtype"] = events[eventId]["desc"][subtypeStart - 1:events[eventId]["desc"].find("min)") + 4]

		# Get the zone in which the event occurred; always use the home team's perspective
		if events[eventId]["type"] == "block":
			if eventTeam == teams["home"] and events[eventId]["desc"].lower().find("off. zone") >= 0:	# home team took shot, blocked by away team in away team's off. zone - probably doesn't happen
				events[eventId]["zone"] = "d"
			elif eventTeam == teams["away"] and events[eventId]["desc"].lower().find("def. zone") >= 0:	# away team took shot, blocked by home team in home team's def. zone
				events[eventId]["zone"] = "d"
			elif eventTeam == teams["home"] and events[eventId]["desc"].lower().find("def. zone") >= 0:	# home team took shot, blocked by away team in away team's def. zone
				events[eventId]["zone"] = "o"
			elif eventTeam == teams["away"] and events[eventId]["desc"].lower().find("off. zone") >= 0:	# away team took shot, blocked by home team in home team's off. zone - probably doesn't happen
				events[eventId]["zone"] = "o"
		else: 
			if eventTeam == teams["home"] and events[eventId]["desc"].lower().find("off. zone") >= 0:	# home team created event (excluding blocked shot) in home team's off. zone
				events[eventId]["zone"] = "o"
			elif eventTeam == teams["away"] and events[eventId]["desc"].lower().find("def. zone") >= 0:
				events[eventId]["zone"] = "o"
			elif eventTeam == teams["home"] and events[eventId]["desc"].lower().find("def. zone") >= 0:
				events[eventId]["zone"] = "d"
			elif eventTeam == teams["away"] and events[eventId]["desc"].lower().find("off. zone") >= 0:
				events[eventId]["zone"] = "d"
			elif events[eventId]["desc"].lower().find("neu. zone") >= 0:
				events[eventId]["zone"] = "n"

		# Record the score at which the event occurred
		# If a goal was scored, increment awayScore or homeScore AFTER recording the score
		#	i.e., for the first goal in the game, both awayScore = 0 and homeScore = 0
		# Exclude shootout goals (this code assumes shootouts only occur in the regular season)

		events[eventId]["awayScore"] = awayScore
		events[eventId]["homeScore"] = homeScore
		if events[eventId]["type"] == "goal":
			if (events[eventId]["period"] <= 4 and events[eventId]["gameId"] < 30000) or events[eventId]["gameId"] >= 30000:
				if events[eventId]["team"] == teams["away"]:
					awayScore = awayScore + 1
				elif events[eventId]["team"] == teams["home"]:
					homeScore = homeScore + 1

		#
		#
		# Parse list of on-ice players
		#
		#

		# Record all skaters and goalie on ice during the play
		tds = r.find_all("td", class_=re.compile("bborder")) # Away players have index=6, home players have index=7
		onIce = [tds[6], tds[7]]

		for i, td in enumerate(onIce):

			onIceSkaters = []
			onIceTeam = ""
			if i == 0:
				onIceTeam = "away"
			elif i == 1:
				onIceTeam = "home"

			for player in td.find_all(attrs={"style" : "cursor:hand;"}):
				position = player["title"][0:player["title"].find(" - ")].lower()
				playerId = players[onIceTeam][int(player.text)]["playerId"]
				if position in ["right wing", "left wing", "center", "defense"]:
					onIceSkaters.append(playerId)
				elif position == "goalie":
					# Store on-ice goalie playerId
					events[eventId][onIceTeam + "G"] = playerId
			
			# Store on-ice skater playerIds
			for j, player in enumerate(onIceSkaters):
				events[eventId][onIceTeam + "S" + str(j + 1)] = player

			# Store number of on-ice skaters
			events[eventId][onIceTeam + "Skaters"] = len(onIceSkaters)

	# Done looping through each play

	#
	#
	# Load the shift files
	#
	#

	# Dictionary to store shift data for output
	shifts = dict()
	shifts["away"] = dict()
	shifts["home"] = dict()

	# If shift html files don't exist locally, download them
	filenames = ["TV0" + str(gameId) + ".HTM", "TH0" + str(gameId) + ".HTM"]
	
	filesExist = True
	for f in filenames:
		if os.path.isfile(inDir + f) == False:
			filesExist = False
				
	if filesExist == False:
		user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.94 Safari/537.36"
		headers = { "User-Agent" : user_agent }
		print "Downloading " + str(filenames)
		for f in filenames:
			nhlUrl = "http://www.nhl.com/scores/htmlreports/" + str(seasonArg) + "/" + f
			savedFile = urllib.urlretrieve(nhlUrl, inDir + f)
		
	print "Processing " + str(filenames)

	# Load each shift html file and parse it
	for i, f in enumerate(filenames):

		# Load shift html file
		htmlFile = file(inDir + f, "r")
		soup = BeautifulSoup(htmlFile.read(), "lxml")
		htmlFile.close()

		if i == 0:
			team = "away"
		elif i == 1:
			team = "home"

		# Get the table containing all players' shift data
		player = ""
		pos = ""
		table = soup.find_all("table", border = "0", cellspacing = "0", cellpadding = "0", width = "100%")[1]

		# Process each row in the shift table
		rows = table.find_all("tr")
		for r in rows:

			# If the tr doesn't have any classes, then check if it's the player name row (if it is, get the jersey number)
			# If the tr contains classes, then check if it contains shift data
			trClasses = r.get("class")
			if trClasses is None:
				firstTdClasses = r.find("td").get("class")
				if firstTdClasses is not None:
					if "playerHeading" in firstTdClasses:
						# Convert jersey numbers into playerIds
						player = r.find("td").text
						player = player[0:player.find(" ")]
						pos = players[team][int(player)]["position"]
						player = players[team][int(player)]["playerId"]
						shifts[team][player] = dict()
						shifts[team][player]["position"] = pos

						# Create a list for each period - each list will contain pairs of shifts times: [start, end]
						for pr in range(1, len(periodDurations) + 1):
							shifts[team][player][pr] = []
			else:
				if ("oddColor" in trClasses) or ("evenColor" in trClasses):
					if len(r.find_all("td")) > 0: # Ignore empty rows
						period = r.find_all("td")[1].text
						start = r.find_all("td")[2].text
						end = r.find_all("td")[3].text
						
						# 2014-2015 regular season OT example: http://www.nhl.com/gamecenter/en/boxscore?id=2014020986 Note that the TOI file just says "OT"
						# 2015-2015 regular season SO example: http://www.nhl.com/gamecenter/en/boxscore?id=2014020999 Note that the TOI file uses "OT" because SO doesn't have shifts 
						# 2013-2014 regular season OT example: http://www.nhl.com/gamecenter/en/boxscore?id=2013020612 Note that the TOI file just says "OT"
						# 2013-2014 playoffs OT1 example: http://www.nhl.com/gamecenter/en/boxscore?id=2013030327 Note that the TOI file uses period 4 for OT1
						# 2013-2014 playoffs OT2 example: http://www.nhl.com/gamecenter/en/boxscore?id=2013030415 Note that the TOI file uses period 5 for OT2
						# Play-by-play periods are always numbered 1, 2, 3, 4, 5, etc. - for regular season, period 5 is the SO
						
						# Cast period to int - if needed, convert regular season OT to period 4
						if period.find("OT") >= 0:
							period = "4"
						period = int(period)
						
						# We only want to output the values formatted as "Elapsed / Game"
						if (start.find(" / ") > -1) and (end.find(" / ") > -1):

							# Convert start time from mm:ss to elapsed seconds
							start = start[0:start.find(" / ")]
							startMin = int(start[0:start.find(":")])
							startSec = int(start[start.find(":")+1:]) + 60 * startMin
							
							# Convert end time from mm:ss to elapsed seconds
							end = end[0:end.find(" / ")]
							endMin = int(end[0:end.find(":")])
							endSec = int(end[end.find(":")+1:]) + 60 * endMin
							
							# Record shift data
							shifts[team][player][period].append([startSec, endSec])

	#
	#
	# Split each player's TOI by score situation and strength situation
	#
	#

	# Create a dictionary to store the TOI breakdown
	# playerId -> strengthSit -> scoreSit
	playerStats = dict()
	scoreSits = [-3, -2, -1, 0, 1, 2, 3]
	strengthSits = ["awayGoaliePulled", "homeGoaliePulled", "away2PP", "home2PP", "awayPP", "homePP", "ev3", "ev4", "ev5"]

	for team in shifts:	
		for player in shifts[team]:
			playerStats[player] = dict()
			for strengthSit in strengthSits:
				playerStats[player][strengthSit] = dict()
				for scoreSit in scoreSits:
					playerStats[player][strengthSit][scoreSit] = dict()

					# Initialize counters for stats
					playerStats[player][strengthSit][scoreSit]["toi"] = 0
					playerStats[player][strengthSit][scoreSit]["ig"] = 0	# individual goals
					playerStats[player][strengthSit][scoreSit]["is"] = 0	# individual shots on goal
					playerStats[player][strengthSit][scoreSit]["im"] = 0	# individual missed shots
					playerStats[player][strengthSit][scoreSit]["ib"] = 0	# individual blocked shots
					playerStats[player][strengthSit][scoreSit]["ia1"] = 0	# individual primary assists
					playerStats[player][strengthSit][scoreSit]["ia2"] = 0	# individual secondary assists
					playerStats[player][strengthSit][scoreSit]["gf"] = 0	# goals for
					playerStats[player][strengthSit][scoreSit]["ga"] = 0	# goals against
					playerStats[player][strengthSit][scoreSit]["sf"] = 0	# shots on goal for
					playerStats[player][strengthSit][scoreSit]["sa"] = 0	# shots on goal against
					playerStats[player][strengthSit][scoreSit]["mf"] = 0	# missed shots for
					playerStats[player][strengthSit][scoreSit]["ma"] = 0	# mised shots against
					playerStats[player][strengthSit][scoreSit]["bf"] = 0	# blocked shots for
					playerStats[player][strengthSit][scoreSit]["ba"] = 0	# blocked shots against

	# Exclude shootouts in the regular season
	endPr = 3
	if gameId < 30000 and len(periodDurations) > 4:
		endPr = 4
	elif gameId >= 30000:
		endPr = len(periodDurations)

	for pr in range(1, endPr + 1):

		# Dictionary to store times a player was on the ice
		# i.e., if 5 skaters were on-ice at t=1, then we'll have five 1's in the flattened list
		sShifts = dict()
		sShifts["away"] = []
		sShifts["home"] = []
		gShifts = dict()
		gShifts["away"] = []
		gShifts["home"] = []
		for team in shifts:								 
			for player in shifts[team]:
				for shift in shifts[team][player][pr]:
					if shifts[team][player]["position"] == "g":
						gShifts[team].extend(range(shift[0], shift[1]))
					else:
						sShifts[team].extend(range(shift[0], shift[1]))

		# List where the value at each index (seconds elapsed) represents the number of skaters/goalies on ice
		sOnIce = dict()
		sOnIce["away"] = [-1] * periodDurations[pr]
		sOnIce["home"] = [-1] * periodDurations[pr]
		gOnIce = dict()
		gOnIce["away"] = [-1] * periodDurations[pr]
		gOnIce["home"] = [-1] * periodDurations[pr]
		for team in teams:
			for t in range(0, periodDurations[pr]):
				sOnIce[team][t] = sShifts[team].count(t)
				gOnIce[team][t] = gShifts[team].count(t)

		# Get the score situation at each second
		# Initialize list where the value at each index (seconds elapsed) represents the goal differential (home - away)
		# Then for each goal, update the difference (starting from the time of the goal to the end of the period)
		pstr = [events[ev] for ev in events if events[ev]["type"] == "pstr" and events[ev]["period"] == pr][0]
		goals = [events[ev] for ev in events if events[ev]["type"] == "goal" and events[ev]["period"] == pr]
		flatScoreSitTimes = [pstr["homeScore"] - pstr["awayScore"]] * periodDurations[pr]
		for g in goals:
			for t in range(g["time"], periodDurations[pr]):
				if g["team"] == teams["home"]:
					flatScoreSitTimes[t] = flatScoreSitTimes[t] + 1
				elif g["team"] == teams["away"]:
					flatScoreSitTimes[t] = flatScoreSitTimes[t] - 1
		
		# Limit the score situation to between -3 and +3 (inclusive)
		flatScoreSitTimes = [max(-3, min(3, t)) for t in flatScoreSitTimes]

		# For each score situation, create a set of times when the score situation occurred
		scoreSitTimes = dict()
		for s in scoreSits:
			scoreSitTimes[s] = set()
		for t in range(0, periodDurations[pr]):
			scoreSitTimes[flatScoreSitTimes[t]].add(t)

		# For each strength situation, create a set of times when the strength situation occurred
		strengthSitTimes = dict()
		for s in strengthSits:
			strengthSitTimes[s] = set()
		for t in range(0, periodDurations[pr]):
			# We're going to treat the score situations as being mutually exclusive
			# This means that if a goalie is pulled at time t, then time t won't be counted towards any ev/pp/pk situations
			# This assumes that we can ignore situations where both goalies are pulled
			aPlayers = gOnIce["away"][t] + sOnIce["away"][t]
			hPlayers = gOnIce["home"][t] + sOnIce["home"][t]
			if gOnIce["away"][t] == 0:
				strengthSitTimes["awayGoaliePulled"].add(t)
			elif gOnIce["home"][t] == 0:
				strengthSitTimes["homeGoaliePulled"].add(t)
			elif aPlayers - hPlayers == 2:
				strengthSitTimes["away2PP"].add(t)
			elif aPlayers - hPlayers == 1:
				strengthSitTimes["awayPP"].add(t)
			elif aPlayers - hPlayers == -1:
				strengthSitTimes["homePP"].add(t)
			elif aPlayers - hPlayers == -2:
				strengthSitTimes["home2PP"].add(t)
			elif aPlayers == hPlayers:
				if aPlayers == 6:
					strengthSitTimes["ev5"].add(t)
				elif aPlayers == 5:
					strengthSitTimes["ev4"].add(t)
				elif aPlayers == 4:
					strengthSitTimes["ev3"].add(t)

		# To get each player's TOI broken down by score situation and strength situation,
		# get the intersection of their shifts, each score situation, and each strength situation
		for team in shifts:								 
			for player in shifts[team]:

				# Create a set of times when the player was on the ice
				shiftTimes = set()
				for shift in shifts[team][player][pr]:
					for t in (range(shift[0], shift[1])):
						shiftTimes.add(t)

				# Record the TOIs
				for strengthSit in strengthSits:
					for scoreSit in scoreSits:
						playerStats[player][strengthSit][scoreSit]["toi"] = playerStats[player][strengthSit][scoreSit]["toi"] + len(set.intersection(strengthSitTimes[strengthSit], scoreSitTimes[scoreSit], shiftTimes))

	#
	#
	# Loop through event data to count each player's stats, broken down by score situation and strength situation
	#
	#

	# We're only keeping track of stats for goals, assists, and corsis
	filteredEvents = [events[ev] for ev in events if events[ev]["type"] in ["goal", "shot", "block", "miss"]]
	pprint(filteredEvents)
	# pprint(playerStats)







