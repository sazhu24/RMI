library(tidyverse)
library(rvest)
library(xml2)
library(httr)
library(rjson)
library(jsonlite)
library(lubridate)

### Social Ads (ignore for now, APIs not connected yet)

## Twitter Ads
twitterClientID <- 'R2pPdkZSMUlmV2xadE5FTGp3VDU6MTpjaQ'
twitterClientSecret <- 'HyzWA_zO0k-2Sq8zH7v925RupDtKZ0IcDbl1j8aF0gJpzdYold'
twitterAccessToken <- '27911608-YQkkJVYPxVpvRWY1Rqy1i2ObDltoFB8jmiZCpRIMD'
twitterTokenSecret <- 'c3OX8hI8hDu5rzswfDGkVtZUkD8Km5FEHcMtZPEoW4cuw'
twitterAPIKey <- 'v0lP01p9SMJpyzyoa46lGnkum'
twitterAPIKeySecret <- 'uDO9U2bJ8rWhxBgoA1hssqDgyejjxh0SjbdAxrU8Vh19E9wLlY'
twitterAppID <- '27474849'
twitterBearerToken <- 'Bearer AAAAAAAAAAAAAAAAAAAAAKE7owEAAAAAAvnDToK5JD3JDb%2B50IcDUnzJ6DA%3DnpLUvvyzIvX1GdhQPYLZsb6MNSe7kQGAzmq1PDOau3TYqYXbaR'

## Meta Ads
metaAppID <- '285621737385240'
metaApppSecret <- '6b97f5c2fffcf90e40144d17920f4028'
metaAccessToken <- 'EAAEDxX4ybRgBAHvhgvzyNEBIXr7KcMTOddv5tMvtOuLA9hZCTmyiZCOiLvcSBfDqQsKCL7CKJP0HeGZBYQCfYcliZCtZCmT3LZCEYYA0tXtPcqr1iquSTYU4rovQRkYP8LIZChlulcLDgI52ZAP7T8WS6vMiTIkslBssMlGH0HGd8ZCV3ikSoYA57yVQPdFUgF3Nng5EcJLQna8CaZAxE4dETt'
metaAdAccountID <- '620605216285817'

# Define keys
app_id = '285621737385240'
app_secret = '6b97f5c2fffcf90e40144d17920f4028'

# Define the app
fb_app <- oauth_app(appname = "facebook",
                    key = app_id,
                    secret = app_secret)

# Get OAuth user access token
fb_token <- oauth2.0_token(oauth_endpoints("facebook"),
                           fb_app,
                           scope = 'public_profile',
                           type = "application/x-www-form-urlencoded",
                           cache = TRUE)

response <- GET("https://graph.facebook.com",
                path = "/UTSEngage",
                query = list(access_token = metaAccessToken))

# Show content returned
fb <- content(response)

### LinkedIn Ads
linkedinID <- '86nzxlaeay1wtm'
linkedinSecret <- 'mjZTo5pp14f3e2di'
linkedInAdToken <- 'AQXG3xfRc6NI7h-Z1rwf1HK38HyIsVg6nwIj18hyEQ2gDKaRjUQk58KdFr1Rg84XyOHfFlOoZqRC7W0fd_3uEl-dpoiyG0woepp2SWnBpjoKgIHdKp5SxL3fgTcu8xWjnijDYzLIn8NE7CH5L_rSX0cHer0vaaDMhAE9Ovvg7gGK3fc5YFhBUksdQuPF9YvOQRP78ERhC3BDUCbS5BS6twF_W10ANyr26ZBO7GSGCWlTmyLMMBAnhfmtSZnZ-HjWSqBcVZ6iBjQmjySfyZbsCdDpOhfd91wsjieeLqz1-Co5EhdZfSgPRZktoft9eUVUK19wUgIS5vygG3YHyQTfg6KvEgoHvg'

