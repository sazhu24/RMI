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

# get all posts since Jan 1, 2023
print('get all posts from social channels dating back to Jan 1, 2023')

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
print('clean response')

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
print('push social media data')

write_sheet(taggedPost, ss = ss, sheet = 'Social - Campaign')
write_sheet(allPosts, ss = ss, sheet = 'Social - All')

