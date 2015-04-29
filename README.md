# ScreamingTwitter
Using Spark Streaming to send tweets from Twitter Streaming API to Elasticsearch

**Requirements:**
- Elasticsearch running on localhost

###To run the project
- Clone the repo
- Create a config file within a config folder with Twitter Credentials. 

**config/TwitterScreamer.conf**
```
  oauthcredentials {
    consumerKey = XXXXXXXXXX
    consumerSecret = XXXXXXXXXX
    accessToken = XXXXXXXXXX
    accessTokenSecret = XXXXXXXXXX
  }
```

- And then run the SBT project from the repo folder 

```
  > sbt run
```
