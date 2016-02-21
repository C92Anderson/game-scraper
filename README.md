# game-scraper

To scrape the first game of the 2015-2016 season, or the first 10 games:
> python scrape-game.py 20152016 20001
>
> python scrape-game.py 20152016 20001-20010

&nbsp;

To get the list of gameIds for a given date (yyyymmdd):
> python get-gameIds.py 20160216

&nbsp;

In some cases, the pbp json will be empty or missing. To use the html pbp report instead of the json pbp:
> python create-fallback-pbp.py 20152016 20194
>
> Before running this, manually prepare the files and Python dicts described by the instructions in create-fallback-pbp.py


