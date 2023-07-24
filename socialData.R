library(tidyverse)
library(rvest)
library(xml2)
library(httr)
library(rjson)
library(jsonlite)
library(lubridate)

### Sprout Social
sproutToken <- 'Bearer ODA1MjE1fDE2ODg3MDIwMTh8ZTcxOTE0YzQtODRlMS00MTMyLWE4M2YtNmRkMzI3YzA4OWE1'
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")
currentDate <- paste(Sys.Date())

## write to this sheet
ss <- 'https://docs.google.com/spreadsheets/d/1_wUKRziRhF90ZfUemHNt3DaOjU2fQlTIPO8rNlA9doY/edit#gid=1033361432' ## OCI

### function - metadata request
getMetadata <- function(url) {
  request <- GET(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                 add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

# get profile IDs
metadeta <- getMetadata(url = 'metadata/customer')
profileIDs <- metadeta[["data"]]

# get all tags
metadeta <- getMetadata(url = 'metadata/customer/tags')
tags <- metadeta[["data"]]

# find campaign tag
campaignTag <- tags %>% filter(text == 'RMI Brand')
tagID <- paste(campaignTag$tag_id)

###
getCall <- function(url, args) {
  request <- POST(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                  body = toJSON(args, auto_unbox = TRUE),
                  add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

# function - post analytics request
sproutPostRequest <- function(page, dateRange){
  
  args <- list("fields" = c(
    "created_time",
    "perma_link",
    "text",
    "internal.tags.id",
    "post_type"),
    "filters" = c("customer_profile_id.eq(3244287, 2528134, 2528107, 2528104)",
                  dateRange),
    "metrics" = c("lifetime.impressions", "lifetime.engagements", 	"lifetime.post_content_clicks", "lifetime.shares_count"), 
    "timezone" = "America/Denver",
    "page" = paste(page))
  
  getStats <- getCall(url = 'analytics/posts', args = args)
  if(is.null(getStats[["paging"]])) {
    postStats <- NULL
  } else {
    metrics <- getStats[["data"]][["metrics"]]
    internal <- getStats[["data"]][["internal"]]
    postStats <- getStats[["data"]] %>% 
      select(-c('metrics', 'internal')) %>% 
      cbind(metrics) %>% 
      cbind(internal)
  }
  return(postStats)
}

allPosts <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', lifetime.impressions = '', lifetime.post_content_clicks = '', 
                       lifetime.engagements = '', lifetime.shares_count = '', lifetime.reactions = '')[0, ]

allPostsTags <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', lifetime.impressions = '', lifetime.post_content_clicks = '', 
                           lifetime.engagements = '', lifetime.shares_count = '', lifetime.reactions = '', id = '')[0, ]

# Pull all posts since Jan 1, 2023
for(i in 1:100){
  stats4 <- sproutPostRequest(i, paste0("created_time.in(2023-01-01T00:00:00..", currentDate, "T23:59:59)")) 
  if(is.null(stats4)){ break }
  
  # data frame - posts with all tags
  stats5 <- stats4 %>% unnest(tags)
  allPostsTags <- allPostsTags %>% rbind(stats5)
  
  # data frame - all posts (unique)
  stats4 <- stats4 %>% select(-tags)
  allPosts <- allPosts %>% rbind(stats4)
}

# clean response
cleanDF <- function(df, type){
  
  posts <- df %>% 
    mutate(engagementRate = round(as.numeric(lifetime.engagements)/as.numeric(lifetime.impressions), 3),
           created_time = as.Date(sub('T(.*)', '', created_time)),
           month = lubridate::month(ymd(created_time), label = TRUE, abbr = FALSE),
           date = paste0(month, ' ', format(created_time,"%d"), ', ', format(created_time,"%Y")),
           icon = '') %>% 
    mutate(across(lifetime.impressions:engagementRate, ~ as.numeric(.x))) %>% 
    filter(!is.na(lifetime.impressions))
  
  for(i in 1:nrow(posts))
  if(grepl('LINKEDIN_COMPANY_UPDATE', posts[i, 'post_type'])){
    posts[i, 'post_type'] <- "LinkedIn"
    posts[i, 'icon'] <- paste(1)
  } else if(grepl('FACEBOOK_POST', posts[i, 'post_type'])){
    posts[i, 'post_type'] <- "Facebook"
    posts[i, 'icon'] <- paste(2)
  } else if(grepl('TWEET', posts[i, 'post_type'])){
    posts[i, 'post_type'] <- "Twitter"
    posts[i, 'icon'] <- paste(3)
  } else if(grepl('INSTAGRAM_MEDIA', posts[i, 'post_type'])){
    posts[i, 'post_type'] <- "Instagram"
    posts[i, 'icon'] <- paste(4)
  }
  
  if(type == 'tagged'){
    posts <- posts %>% 
      mutate(icon = as.numeric(icon)) %>% 
      select(created_time, date, post_type, icon, id, text, impressions = lifetime.impressions, 
             engagements = lifetime.engagements, engagementRate, shares = lifetime.shares_count, perma_link)
  } else if(type == 'all'){
    posts <- posts %>% 
      mutate(icon = as.numeric(icon)) %>% 
      select(created_time, date, post_type, icon, text, impressions = lifetime.impressions, 
             engagements = lifetime.engagements, engagementRate, shares = lifetime.shares_count, perma_link) 
  }

}

allPosts <- cleanDF(allPosts, 'all')
taggedPosts <- cleanDF(allPostsTags, 'tagged')

##
taggedPosts <- allPosts %>% 
  filter(created_time == '2023-04-06' & grepl('OCI', text)) 

# set LinkedIn metrics thresholds
maxImpressions <- taggedPosts[which(taggedPosts[,'post_type'] == 'LinkedIn'), 'impressions'] * 1.5
maxEngagements <- taggedPosts[which(taggedPosts[,'post_type'] == 'LinkedIn'), 'engagements'] * 1.7
maxEngagementRate <- taggedPosts[which(taggedPosts[,'post_type'] == 'LinkedIn'), 'engagementRate'] * 1.6

taggedPosts <- taggedPosts %>% 
  mutate(maxIM = maxImpressions,
         maxEG = maxEngagements,
         maxER = maxEngagementRate)

# push data
write_sheet(taggedPost, ss = ss, sheet = 'Social - Campaign')
write_sheet(allPosts, ss = ss, sheet = 'Social - All')

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




