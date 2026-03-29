# Mobile App Dev DB
## ISLab project to gather info about mobile app developers
> [!NOTE]
This is a WIP.  
Using Google play scraper to pull data from google play store[^1].  
Pulling App Store info off of this old API from 2017[^2].
## Todo
- ~~Flesh out the schema~~
- google-play-scraper not pulling enough apps, need to change from `search` to `collection` for better results
- ~~Rewrite crawling logic~~
- ~~Dev as model?~~
- ~~Include app version in schema~~
- Cloud compatibility
- Run continuously on Lab PC
- Pull even more apps
## To run
```shell=
pip install -r requirements.txt
docker compose up -d
python3 main.py
psql -h localhost -p 5433 -U crawler -d appcrawler
(passwrd: crawlerpass)
```
[^1]: https://github.com/JoMingyu/google-play-scraper
[^2]: https://performance-partners.apple.com/search-api