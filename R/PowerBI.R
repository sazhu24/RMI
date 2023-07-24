library(tidyverse)
library(xml2)
library(httr)
library(rjson)
library(jsonlite)
library(lubridate)
library(openxlsx)
library(googledrive)
library(googlesheets4)
library(googleAnalyticsR)
library(salesforcer)
library(rvest)

## set Google authentication token
options(gargle_oauth_cache = ".secrets")
gs4_auth(cache = ".secrets", email = "sazhu24@amherst.edu")

## push data to this Google sheet
ss <- 'https://docs.google.com/spreadsheets/d/1BzLkm4jZr1WwMQsC4hBweKkvnBsWq4ZXlbyg7BYMsoA/edit?usp=sharing' ## OCI

### Monday.com
print('GET MONDAY.COM DATA')

# API Token
mondayToken <- 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI2NDQzMjQyNiwiYWFpIjoxMSwidWlkIjozNTY3MTYyNSwiaWFkIjoiMjAyMy0wNi0yMlQxNjo0NTozOS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjY4NTM2NiwicmduIjoidXNlMSJ9.KsZ9DFwEXeUuy23jRlCGauiyopcUFTHF6WciunTLFLM'

getMondayCall <- function(x) {
  request <- POST(url = "https://api.monday.com/v2",
                  body = list(query = x),
                  encode = 'json',
                  add_headers(Authorization = mondayToken)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

# get Active Projects Board
print('get Active Projects board from Monday.com')

query <- "query { boards (ids: 2208962537) { items { id name column_values{ id value text } } } } "
res <- getMondayCall(query)
activeProjects <- as.data.frame(res[["data"]][["boards"]][["items"]][[1]])

# iterate through APB to find projects with "Metrics Dashboard" in the promotion tactics column
print('find Metrics Dashboard projects')

campaigns <- data.frame(id = '', row = '', name = '')[0,]
for(i in 1:nrow(activeProjects)){
  #i <- 1
  board <- activeProjects[[3]][[i]]
  if(grepl('Metrics Dashboard', paste(board[11, 'text']))){
    campaigns <- campaigns %>% 
      rbind(c(paste(activeProjects[i, 'id']), i, c(paste(activeProjects[i, 'name'])))) 
  }
}
names(campaigns) <- c('id', 'row', 'name')

# filter to identify campaign of interest
print('find target campaign and dashboard columns')

targetCampaign <- campaigns %>% 
  filter(grepl('CIP: Coal v Gas campaign', name))

# get metrics, audiences, and ID
campaignRow <- as.numeric(targetCampaign[1, 'row'])
campaignBoard <- activeProjects[[3]][[campaignRow]]
campaignDF <- data.frame(campaignID = campaignBoard[16, 'text'], 
                         campaignAudiences = campaignBoard[15, 'text'], 
                         campaignMetrics = campaignBoard[11, 'text'])

# push data
print('push monday.com data')

write_sheet(campaignDF, ss = ss, sheet = 'Campaign Overview')

### WEB
print('GET GOOGLE ANALYTICS DATA')

## set GA credentials and property ID
ga_auth(email = "sara.zhu@rmi.org")
property_id <- 354053620
metadataGA4 <- ga_meta(version = "data", property_id)
currentDate <- Sys.Date()
dates1 <- c("2023-01-01", paste(currentDate))

## look for content group tag - not set up yet
# contentGroup <- ga_data(
#   property_id,
#   metrics = c("engagedSessions", "sessions", "engagementRate", "averageSessionDuration", "screenPageViews", "newUsers", "totalUsers"),
#   dimensions = c("contentGroup", 'pageTitle', 'pagePath'),
#   date_range = dates1,
#   limit = -1
# ) %>% 
#   filter(contentGroup != "(not set)") %>% 
#   filter(pageTitle != '')
# 
# groupPages <- contentGroup$pageTitle

## OCI+ Campaign
pageTitles <- c('Top Strategies to Cut Dangerous Methane Emissions from Landfills - RMI',
                'OCI+ Update: Tackling Methane in the Oil and Gas Sector - RMI',
                'Clean Energy 101: Methane-Detecting Satellites - RMI',
                'Waste Methane 101: Driving Emissions Reductions from Landfills - RMI',
                'Key Strategies for Mitigating Methane Emissions from Municipal Solid Waste - RMI',
                'Know Your Oil and Gas - RMI',
                'Intel from Above: Spotting Methane Super-Emitters with Satellites - RMI')

## Get Full URLs 
getLinks <- ga_data(
  property_id,
  metrics = c("sessions", "totalUsers", "engagementRate", "userEngagementDuration", "screenPageViews"),
  dimensions = c("pageTitle", 'fullPageUrl'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pageTitles),
  limit = -1
) %>% 
  mutate(fullPageUrl = paste0('https://', fullPageUrl))

pages <- getLinks$pageTitle # get list of page titles
linkTitles <- getLinks$pageTitle # get list of page titles
linkUrls <- getLinks$fullPageUrl # get list of page URLs

## get page type by scraping website metadata for each page
print('scrape website metadata')

pageData <- data.frame(linkTitles, linkUrls, metadata1 = '', metadata2 = '')
for(i in 1:nrow(pageData)){
  
  url <- paste(pageData[i, 'linkUrls'])
  tryCatch( { 
    url_tb <- url %>%
      read_html() %>% 
      html_nodes('script') %>% 
      html_text() %>% 
      as.data.frame() %>% 
      rename(node = 1) %>% 
      filter(grepl('schema.org', node)) %>% 
      mutate(keywords = sub('.*keywords\\"\\:\\[', "", node),
             keywords = gsub('\\].*', "", keywords))
    
    pageData[i, 'metadata1'] <- url_tb[1, 'keywords']
    pageData[i, 'metadata2'] <- url_tb[2, 'keywords']
    
  }, error = function(e){
    pageData[i, 'metadata1'] <- NA
    pageData[i, 'metadata2'] <- NA
  })
  
}

pageData <- pageData %>% 
  filter(!is.na(metadata1) & metadata1 != '')

pageData <- pageData[!duplicated(linkTitles),]

pageType <- pageData %>% 
  mutate(pageType = ifelse(grepl('article', tolower(metadata2)), 'Article',
                           ifelse(grepl('report', tolower(metadata2)), 'Report', '')))


## get web traffic and key metrics for all pages
print('get page web traffic')

campaignPages <- ga_data(
  property_id,
  metrics = c('screenPageViews', "totalUsers", "userEngagementDuration", 'conversions', 'conversions:form_submit', 'conversions:file_download', 'conversions:click'),
  dimensions = c("pageTitle"),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  mutate(engagementDuration = userEngagementDuration / totalUsers,
         sec = round(engagementDuration %% 60, 0),
         min = (engagementDuration / 60) |> floor(),
         avgEngagementDuration = paste0(min, ':', sec)) %>% 
  select(screenPageViews, totalUsers, engagementDuration, conversions, form_submits = 'conversions:form_submit', downloads = 'conversions:file_download', clicks = 'conversions:click') %>% 
  left_join(select(pageType, c(pageTitle = linkTitles, pageType)), by = c('pageTitle')) 

pages <- campaignPages$pageTitle # get list of page titles

campaignPages <- campaignPages %>% # clean end of title
  mutate(pageTitle = gsub(' - RMI', '', pageTitle))

## get page views by region
trafficByRegion <- ga_data(
  property_id,
  metrics = c('screenPageViews'),
  dimensions = c("pageTitle", 'region', 'country'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  filter(screenPageViews > 4) %>% 
  arrange(pageTitle) %>% 
  mutate(pageTitle = gsub(' - RMI', '', pageTitle))

## get acquisition

# function - correct social media and email channel traffic
correctTraffic <- function(df, type){
  if(type == 'session'){
    df <- df %>% 
      rename(medium = sessionMedium) %>% 
      rename(source = sessionSource) %>% 
      rename(defaultChannelGroup = sessionDefaultChannelGroup)
  } 
  
  df <- df %>% 
    mutate(pageTitle = gsub(' - RMI', '', pageTitle)) %>% 
    mutate(medium = ifelse(grepl('mail.google.com', source)|grepl('web-email|sf|outlook', medium), 'email', medium),
           source = ifelse(grepl('linkedin', source), 'linkedin', source),
           source = ifelse(grepl('facebook', source), 'facebook', source),
           source = ifelse(grepl('dlvr.it|twitter', source)|source == 't.co', 'twitter', source),
           medium = ifelse(grepl('linkedin|lnkd.in|facebook|twitter|instagram', source)|grepl('twitter|fbdvby', medium), 'social', medium),
           medium = ifelse(grepl('/t.co/', pageReferrer), 'social', medium),
           source = ifelse(grepl('/t.co/', pageReferrer), 'twitter', source),
           defaultChannelGroup = ifelse(medium == 'social', 'Organic Social', 
                                        ifelse(medium == 'email', 'Email', defaultChannelGroup))) 
  
  return(df)
}

# get aquisition - sessions
aquisitionSessions <- ga_data(
  property_id,
  metrics = c("sessions"),
  dimensions = c("pageTitle", "sessionSource", "sessionMedium", "pageReferrer", 'sessionDefaultChannelGroup'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  arrange(pageTitle)
  
aquisitionSessions <- correctTraffic(aquisitionSessions, 'session') %>% 
  group_by(pageTitle, defaultChannelGroup) %>% 
  summarize(sessions = sum(sessions))

# get aquisition - conversions
aquisitionConversions <- ga_data(
  property_id,
  metrics = c("conversions", 'conversions:form_submit', 'conversions:file_download', 'conversions:click'),
  dimensions = c("pageTitle", "source", "medium", 'pageReferrer', 'defaultChannelGroup'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  select(pageTitle, source, medium, conversions, pageReferrer, defaultChannelGroup, form_submit = 'conversions:form_submit', download = 'conversions:file_download', click = 'conversions:click') %>% 
  arrange(pageTitle)

aquisitionConversions <- correctTraffic(aquisitionConversions, 'conversion') %>% 
  group_by(pageTitle, defaultChannelGroup) %>% 
  summarize(conversions = sum(conversions),
            downloads = sum(form_submit),
            form_submits = sum(download),
            clicks = sum(click)) 

# bind conversions to sessions
aquisitionAll <- aquisitionSessions %>% 
  left_join(aquisitionConversions, by = c('pageTitle', 'defaultChannelGroup'))

# get acquisition - social
aquisitionSocial <- ga_data(
  property_id,
  metrics = c("sessions", "screenPageViews"),
  dimensions = c("pageTitle", "sessionSource", "sessionMedium", 'pageReferrer', 'sessionDefaultChannelGroup'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  arrange(pageTitle) 

aquisitionSocial <- correctTraffic(aquisitionSocial, type = 'session') %>% 
  filter(medium == 'social') %>% 
  group_by(pageTitle, source) %>% 
  summarize(sessions = sum(sessions),
            screenPageViews = sum(screenPageViews)) 

# push google analytics data
print('push google analytics data')

write_sheet(campaignPages, ss = ss, sheet = 'Web - All Pages')
write_sheet(trafficByRegion, ss = ss, sheet = 'Web - Region')
write_sheet(aquisitionAll, ss = ss, sheet = 'Web - Acquisition')
write_sheet(aquisitionSocial, ss = ss, sheet = 'Web - Social Traffic')

## get email stats and push to dataset
getStoryStats <- function(campaign){
  
  # find link on Email Stats spreadsheet
  df <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Spark Stats (Unformatted)') %>% 
    mutate(date = as.Date(date))
  
  df1 <- df %>%
    filter(grepl(paste(linkUrls, collapse = '|'), url_1)) %>% 
    select(c(1:22))
  
  colnames(df1)[c(19:22)] <- c("story_url", "story_title", "story_clicks", "story_COR")
  
  df2 <- df %>%
    filter(grepl(paste(linkUrls, collapse = '|'), url_2)) %>% 
    select(c(1:18, 23:26))
  
  colnames(df2)[c(19:22)] <- c("story_url", "story_title", "story_clicks", "story_COR")
  
  df3 <- df %>%
    filter(grepl(paste(linkUrls, collapse = '|'), url_3)) %>% 
    select(c(1:18, 27:30))
  
  colnames(df3)[c(19:22)] <- c("story_url", "story_title", "story_clicks", "story_COR")
  
  allStoryStats <- df1 %>% rbind(df2) %>% rbind(df3) %>% 
    mutate(emailTitle = paste0('NL ', date, ': ', subject_line),
           date = as.Date(date),
           icon = '',
           story_title = gsub(' - RMI', '', story_title)) 
  
  allStoryStats <- allStoryStats[rev(order(allStoryStats$date)),]
  allStoryStats[, 'icon'] <- as.numeric(rownames(allStoryStats))
  
  # push data
  print('push email stats data')
  
  write_sheet(df, ss = ss, sheet = 'All Email Stats')
  write_sheet(allStoryStats, ss = ss, sheet = 'Campaign Email Stats')
  
}

getStoryStats()

### SOCIAL MEDIA
print('GET SOCIAL MEDIA DATA')

## Sprout Social API Token
sproutToken <- 'Bearer ODA1MjE1fDE2ODg3MDIwMTh8ZTcxOTE0YzQtODRlMS00MTMyLWE4M2YtNmRkMzI3YzA4OWE1'
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")
currentDate <- paste(Sys.Date())

## function - metadata request
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

# function - make call
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


### SALESFORCE
print('GET SALESFORCE DATA')

## define campaign variables
campaigns_reports <- c('7016f0000023VTHAA2', '7016f0000013txRAAQ', '7016f0000023TgYAAU', '7016f0000013vm9AAA') 

campaigns_events <- c('7016f0000023VTgAAM') # WBN: OCI +

campaignEmailIDs <- c(1389821559, 1327603672)

methanelinks <- c('https://rmi.org/oci-update-tackling-methane-in-the-oil-and-gas-sector/',
                  'https://rmi.org/clean-energy-101-methane-detecting-satellites/',
                  'https://rmi.org/waste-methane-101-driving-emissions-reductions-from-landfills/')

## Pardot API Request Headers
token4 <- 'Bearer 00DU0000000HJDy!ARAAQJgwh2XkV39UIVg2Z99uwmN8TSRVUX5X35Hh31f6keFdINEUSQPaZYNspHJoydVA7dmirNBYPl.2recURpJoFsA.EpZz'
token5 <- "Bearer 00DU0000000HJDy!ARAAQHBeCYE32ms1gj3R73nHD67hGKoheCUz4kNZyJOFwqfIcZ1y9DhWr7QEBE2k6xjtanaIqen.orUapCRWW2Eb0QkqLfmH"
header4 <- c("Authorization" = token4, "Pardot-Business-Unit-Id" = "0Uv0B000000XZDkSAO")
header5 <- c("Authorization" = token5, "Pardot-Business-Unit-Id" = "0Uv0B000000XZDkSAO")

## FUNCTIONS

## get email clicks for campaign
get <- function(url, header) {
  request <- GET(url, add_headers(.headers = header))
  response <- jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8"))
}

## get clicks per email
getBatchClicks <- function(emailId, df){
  allClicks <- data.frame(id = '', prospect_id = '', url = '', list_email_id = '', email_template_id = '', created_at = '')[0,]
  
  for(i in 1:25){
    if(i == 1){
      email_clicks <- GET(paste0("https://pi.pardot.com/api/emailClick/version/4/do/query?format=json&list_email_id=", df[emailId, 'id']),
                          add_headers(.headers = header4))
      getClicks <- jsonlite::fromJSON(content(email_clicks, as = "text", encoding = "UTF-8"))
      clicksQuery <- getClicks[["result"]][["emailClick"]]
      
      digit <- sub('(.*) ', '', str_match(clicksQuery[200, 'created_at'], " *(.*?)\\s*(:)")[,2])
      if(grepl('^0', digit)){ hour <- '%2' } else { hour <- '%20' }
      nextDate <- gsub(' ', hour, clicksQuery[200, 'created_at'])
      
      allClicks <- allClicks %>% rbind(clicksQuery)
    } else {
      email_clicks <- GET(paste0("https://pi.pardot.com/api/emailClick/version/4/do/query?format=json&created_after=", nextDate, "&list_email_id=", df[emailId, 'id']),
                          add_headers(.headers = header5))
      getClicks <- jsonlite::fromJSON(content(email_clicks, as = "text", encoding = "UTF-8"))
      clicksQuery <- as.data.frame(getClicks[["result"]][["emailClick"]])
      
      allClicks <- allClicks %>% rbind(clicksQuery)
      if (nrow(clicksQuery) < 200) break
      
      digit <- sub('(.*) ', '', str_match(clicksQuery[200, 'created_at'], " *(.*?)\\s*(:)")[,2])
      if(grepl('^0', digit)){ hour <- '%2' } else { hour <- '%20' }
      nextDate <- gsub(' ', hour, clicksQuery[200, 'created_at'])
      
    }
  }
  return(allClicks)
}

## get email clicks for all emails dating back to 01/2022
getProspectClicks <- function(df){
  clicksTotal <- data.frame(id = '', prospect_id = '', url = '', list_email_id = '', email_template_id = '', created_at = '')[0,]
  
  for(i in 1:nrow(df)){
    clicks <- getBatchClicks(i, df)
    clicksTotal <- clicksTotal %>% rbind(clicks)
  }
  
  return(clicksTotal)
}

## get all prospects (all contacts and leads with Pardot Activity)
getProspects <- function() {
  
  my_soql <- sprintf("SELECT Id, Name, Account_Name_Text__c, Title, Email,
                             pi__url__c, pi__score__c, pi__last_activity__c,
                             MailingCountry, MailingState, MailingCity, Account_Record_Type__c
                           
                    FROM Contact
                    WHERE pi__last_activity__c != null 
                    ORDER BY pi__last_activity__c DESC")
  
  contactsQuery <- sf_query(my_soql, 'Contact', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Account_Name_Text__c, Title, Email, 
           Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c,
           Country = MailingCountry, State = MailingState, City = MailingCity) %>% 
    mutate(RecordType = 'Contact')
  
  my_soql <- sprintf("SELECT Id,  Name, Company, Title, Industry, Email,
                           pi__url__c, pi__score__c, pi__last_activity__c,
                           Country, State, City

                    FROM Lead
                    WHERE pi__last_activity__c != null 
                    ORDER BY pi__last_activity__c DESC")
  
  leadsQuery <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Title, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c,
           Country, State, City) %>% 
    mutate(RecordType = 'Lead')
  
  prospects <- contactsQuery %>% rbind(leadsQuery) ## 209,614
  
  # identify and remove duplicates
  dup <- prospects[duplicated(prospects[,c("Pardot_URL")]) | duplicated(prospects[,c("Pardot_URL")], fromLast = TRUE), ] 
  
  dup <- dup %>% 
    mutate(Account = ifelse(grepl('unknown|not provided', tolower(Account))|Account == '', NA, Account)) %>% 
    filter(is.na(Account)|Account != '') %>% 
    filter(RecordType == 'Lead') %>% 
    distinct(Pardot_URL, .keep_all = TRUE)
  
  prospects_bad <- prospects %>% filter(!is.na(Email)) %>% anti_join(dup)
  unique_prospects <- prospects_bad[!duplicated(prospects_bad[, 'Pardot_URL']), ]
  
  return(unique_prospects)
}

## get all accounts
getAllAccounts <- function(){
  
  my_soql <- sprintf("SELECT Id, Name, Type, Industry, Email_Domain__c, D_B_Business_Name__c, D_B_Company_Description__c, 
                      D_B_Major_Industry_Category_Name__c, D_B_NAICS_Description_1__c, D_B_NAICS_Description_2__c,
                      D_B_SIC4_Code_1_Description__c, D_B_SIC4_Code_2_Description__c,
                      Top_Account__c, Total_Opportunity_Payments__c, of_Gifts__c, Number_of_Open_Opportunities__c,
                      Prospect_Program_Interests__c, Affinity__c, Program_Interest_Multi_Select_Text__c, Website, D_B_Web_Address__c, X18_Char_ID__c

                    FROM Account")
  all_accounts <- sf_query(my_soql, "Account", api_type = "Bulk 1.0")
  return(all_accounts)
}

##### REPORTS AND EVENTS

# get list of campaigns
my_soql <- sprintf("SELECT Id, 
                           Name,
                           Type,
                           Category__c,
                           CreatedDate,
                           Description,
                           Contact_Report__c,
                           Members_in_Campaign__c
                    FROM Campaign
                    WHERE CreatedDate > 2022-01-01T01:02:03Z")
campaign_list <- sf_query(my_soql) %>% 
  select(Id, Name, Type, CreatedDate, Members = Members_in_Campaign__c)

# get list of all reports
reports <- campaign_list %>% 
  filter(Type == 'Report') %>% 
  filter(grepl('^RP', Name)) %>% 
  filter(Members > 10)

# get list of all events
events <- campaign_list %>% 
  filter(grepl('Event|Training|Workshop', Type) & Type != 'Development Event')

# get campaign members and join to contact/lead/account info
getCampaignMembers <- function(campaignIdList, campaignType) {

  # get campaign members
  if(length(campaignIdList) == 0){
    return()
  } else{
    
  }
  my_soql <- sprintf("SELECT CampaignId,
                             Name,
                             Status,
                             HasResponded,
                             ContactId,
                             LeadId,
                             AccountId,
                             CreatedDate

                      FROM CampaignMember
                      WHERE CampaignId in ('%s')",
                     paste0(campaignIdList, collapse = "','"))
  campaign_members <- sf_query(my_soql, 'CampaignMember', api_type = "Bulk 1.0") %>% 
    left_join(select(campaign_list, c(CampaignId = Id, CampaignName = Name)), by = 'CampaignId') %>% 
    filter(!(is.na(as.character(ContactId)) & is.na(LeadId)))
  
  campaignContacts <- campaign_members %>% filter(!is.na(ContactId)) %>% distinct(ContactId, .keep_all = TRUE)
  campaignLeads <- campaign_members %>% filter(!is.na(LeadId)) %>% distinct(LeadId, .keep_all = TRUE)
  
  
  # contacts in campaign
  my_soql <- sprintf("SELECT Id, Name, Account_Name_Text__c, AccountId, Title, Email,
                             pi__url__c, pi__score__c, pi__last_activity__c,
                             Giving_Circle__c, Top_Philanthropic_and_Wealth_Lists__c,
                             Account_Largest_Gift__c, Number_of_Days_since_Last_Gift_or_SC__c,
                             MailingCountry, MailingState, MailingCity
                             
                      FROM Contact
                      WHERE Id in ('%s')
                      ORDER BY pi__last_activity__c DESC NULLS LAST", 
                     paste0(campaignContacts$ContactId, collapse = "','"))
  
  campaignContactsQuery <- sf_query(my_soql, 'Contact', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Account_Name_Text__c, Title, Email, 
           Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c,
           Country = MailingCountry, State = MailingState, City = MailingCity, 
           Giving_Circle = Giving_Circle__c, Top_Philanthropic = Top_Philanthropic_and_Wealth_Lists__c, Last_Gift = Number_of_Days_since_Last_Gift_or_SC__c,
           AccountId) %>% 
    mutate(RecordType = 'Contact') %>% 
    filter(!is.na(AccountId))
  
  # leads in campaign
  my_soql <- sprintf("SELECT Id, Name, Company, Title, Email,
                           pi__url__c, pi__score__c, pi__last_activity__c,
                           Country, State, City

                    FROM Lead
                    WHERE Id in ('%s')
                    ORDER BY pi__last_activity__c DESC NULLS LAST", 
                     paste0(campaignLeads$LeadId, collapse = "','"))
  
  campaignLeadsQuery <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Title, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c, Country, State, City) %>% 
    mutate(Giving_Circle = '',
           Top_Philanthropic = '',
           Last_Gift = '',
           RecordType = 'Lead')
  
  # accounts in campaign (contacts)
  my_soql <- sprintf("SELECT Id, Name, Type, Industry, Email_Domain__c, D_B_Business_Name__c, D_B_Company_Description__c, 
                      D_B_Major_Industry_Category_Name__c, D_B_NAICS_Description_1__c, D_B_NAICS_Description_2__c,
                      D_B_SIC4_Code_1_Description__c, D_B_SIC4_Code_2_Description__c,
                      Top_Account__c, Total_Opportunity_Payments__c, of_Gifts__c, Number_of_Open_Opportunities__c,
                      Prospect_Program_Interests__c, Affinity__c, Program_Interest_Multi_Select_Text__c

                      FROM Account 
                      WHERE Id in ('%s')", 
                     paste0(campaignContactsQuery$AccountId, collapse = "','"))
  campaignContactAccounts <- sf_query(my_soql, 'Account', api_type = "Bulk 1.0") %>% 
    select(AccountId = Id, Account = Name, AccountType = Type, Industry, AccountDomain = Email_Domain__c,
           TotalGiving = Total_Opportunity_Payments__c, NumGifts = of_Gifts__c, NumOpenOpps = Number_of_Open_Opportunities__c,
           DB_IndustryCategory = D_B_Major_Industry_Category_Name__c, NAICS1 = D_B_NAICS_Description_1__c, SIC1 = D_B_SIC4_Code_1_Description__c)
  
  campaignContactsQuery <- campaignContactsQuery %>% 
    left_join(select(campaignContactAccounts, -Account), by = c('AccountId'))
  
  campaignLeadsQuery <- campaignLeadsQuery %>% 
    left_join(campaignContactAccounts, by = c('Account')) %>% 
    relocate(AccountId, .after = 'Last_Gift')
  
  contactsLeads <- campaignContactsQuery %>%
    rbind(campaignLeadsQuery) %>% 
    mutate(Domain = sub("(.*)\\@", "", Email)) %>% 
    relocate(Domain, .after = "Email") %>% 
    relocate(RecordType, .after = Id) 
  
  CampaignMembers <- campaign_members %>% 
    mutate(Id = ifelse(is.na(ContactId), LeadId, ContactId)) %>% 
    select(CampaignId, CampaignName, Name, Id, Status, CreatedDate) %>% 
    left_join(contactsLeads, by = c('Id', 'Name'))
  
  if(campaignType == 'Event'){
    CampaignMembers <- CampaignMembers %>% mutate(EngagementType = 'Event')
  } else if (campaignType == 'Report'){
    CampaignMembers <- CampaignMembers %>% mutate(EngagementType = 'Report Download')
  }
  
  return(CampaignMembers)
  
}

print('get event campaigns')
campaignMembersEvents <- getCampaignMembers(campaigns_events, 'Event')

print('get report campaigns')
campaignMembersReports <- getCampaignMembers(campaigns_reports, 'Report') 


##### GET EMAIL CLICKS
print('get email clicks')

## get all prospects
prospects <- getProspects() %>% 
  mutate(Domain = sub("(.*)\\@", "", Email))

## get all accounts
all_accounts <- getAllAccounts() %>% 
  select(AccountId = Id, Account = Name, AccountType = Type, Industry, AccountDomain = Email_Domain__c,
         Website, DB_Website = D_B_Web_Address__c, DB_IndustryCategory = D_B_Major_Industry_Category_Name__c, NAICS1 = D_B_NAICS_Description_1__c, 
         NAICS2 = D_B_NAICS_Description_2__c, SIC1 = D_B_SIC4_Code_1_Description__c, SIC2 = D_B_SIC4_Code_2_Description__c,
         TopAccount = Top_Account__c, TotalGiving = Total_Opportunity_Payments__c, NumGifts = of_Gifts__c, NumOpenOpps = Number_of_Open_Opportunities__c)

## get domain info for gov accounts
domainKey <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/domainKey.xlsx')

## get audience domains and accounts
audienceDomainsAccounts <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/audienceDomainsAccounts.xlsx')

emailStats <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=450080785', sheet = 'All Spark Stats (Unformatted)') %>% 
  mutate(id = as.numeric(id),
         emailName = name,
         name = paste0('NL ', date, ': ', subject_line))

## filter by email of interest
emailIDs <- data.frame(id = campaignEmailIDs)

## get clicks for email of interest
clicksAll <- getProspectClicks(emailIDs)

## clean and filter links
linkClicks <- clicksAll %>% 
  mutate(url = sub('\\?(.*)', '', url)) %>% 
  filter(grepl(paste(methanelinks, collapse = '|'), url))

accountsUnique <- all_accounts[!duplicated(all_accounts$Account) & !duplicated(all_accounts$Account, fromLast = TRUE),] %>% 
  filter(!grepl('unknown|not provided|contacts created by revenue grid', Account))

# clean clicks df
clicksByProspect <- select(linkClicks, c(emailId = list_email_id, Pardot_URL = prospect_id, url, created_at)) %>% 
  mutate(Pardot_URL = paste0("http://pi.pardot.com/prospect/read?id=", as.character(Pardot_URL)),
         CreatedDate = as.Date(created_at, format="%Y-%m-%d")) %>% 
  left_join(prospects, by = c('Pardot_URL')) %>% 
  mutate(Status = 'Email',
         Domain = sub("(.*)\\@", "", Email),
         EngagementType = 'Newsletter Click') %>% 
  left_join(select(domainKey, c(domain, domainCount = count, domainName = name, domainLevel = level)), by = c('Domain' = 'domain')) %>% 
  left_join(select(emailStats, c(emailId = id, emailName)), by = 'emailId') %>% 
  mutate(CampaignName = gsub('Spark', '', emailName), Giving_Circle = '', Top_Philanthropic = '', Last_Gift = '') %>% 
  filter(!is.na(Domain)) %>% 
  mutate(Account = ifelse(grepl('unknown|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account)) %>% 
  left_join(select(accountsUnique, c(AccountsName = Account, AccountDomain, Industry, AccountType, TotalGiving, 
                                   NumGifts, NumOpenOpps, AccountId, DB_IndustryCategory, NAICS1, SIC1)), 
            by = c('Account' = 'AccountsName')) %>% 
  select(CampaignName, EngagementType, Id, RecordType, Status, CreatedDate, Name, Title, Domain, Email, Account, AccountType, Industry,
         TotalGiving, NumGifts, NumOpenOpps, Pardot_Score, Pardot_LastActivity, Pardot_URL, Country, State, City, Giving_Circle, Top_Philanthropic, Last_Gift, AccountId)

# combine campaigns and clean 
df <- campaignMembersEvents %>% 
  rbind(campaignMembersReports) %>% 
  mutate(CreatedDate = as.Date(CreatedDate, format = "%Y-%m-%d"),
         Status = ifelse(grepl('Register', Status), 'Registered - DNA', Status)) %>% 
  select(CampaignName, EngagementType, Id, RecordType, Status, CreatedDate, Name, Title, Domain, Email, Account, AccountType, Industry, 
         TotalGiving, NumGifts, NumOpenOpps, Pardot_Score, Pardot_LastActivity, Pardot_URL, Country, State, City, Giving_Circle, Top_Philanthropic, Last_Gift, AccountId) %>% 
  filter(grepl('Register|Attended|Download|Email', Status) & !is.na(Domain)) %>% 
  rbind(clicksByProspect) %>% 
  left_join(select(all_accounts, c(AccountsName = Account, Domain = AccountDomain, AccountsIndustry = Industry, AccountType2 = AccountType)), by = c('Domain')) %>% 
  mutate(Account = ifelse(grepl('unknown|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account),
         Industry = ifelse(Account == 'Unknown' | !is.na(Industry), AccountsIndustry, Industry),
         AccountType = ifelse(Account == 'Unknown' | !is.na(AccountType), AccountType2, AccountType),
         Account = ifelse(Account == 'Unknown' & !is.na(AccountsName), AccountsName, Account),
         Account = ifelse(is.na(Account), 'Unknown', Account)) %>% 
  mutate(AccountType = ifelse(!is.na(AccountType), AccountType, 
                              ifelse(grepl('\\.gov|\\.co\\.us', Domain), 'Government', 
                                     ifelse(grepl('org$', Domain) & !grepl('rmi\\.org|third-derivative', Domain), 'Organization', 
                                            ifelse(grepl('edu$', Domain), 'Academic', 
                                                   ifelse(grepl('Household', Account), 'Household', AccountType)))))
  ) %>% 
  left_join(select(domainKey, c(govDomain = domain, level, govName = name)), by = c('Domain' = 'govDomain')) %>%
  mutate(Audience1 = ifelse(level == 'FEDERAL', 'National Gov.',
                            ifelse(level == 'STATE'|grepl('state of|commonwealth of', tolower(Account)), 'State Gov.',
                                   ifelse(level == 'LOCAL'|level == 'COUNTY'|grepl('city of|county of', tolower(Account)), 'Local Gov.',
                                          ifelse(level == 'INTERNATIONAL', 'International Gov.', '')))),
         Audience1 = ifelse((grepl(audienceDomainsAccounts[1, 'multilateralDomains'], Domain)|grepl(audienceDomainsAccounts[1, 'multilateralAccounts'], Account)), 'Multilateral Institution',
                            ifelse((grepl(audienceDomainsAccounts[1, 'NGODomains'], Domain)|grepl(audienceDomainsAccounts[1, 'NGOAccounts'], Account)), 'NGO',
                                   ifelse((grepl(audienceDomainsAccounts[1, 'financialDomains'], Domain)|grepl(audienceDomainsAccounts[1, 'financialAccounts'], Account)), 'Financial Entity', Audience1))),
         Audience1 = ifelse(grepl('Corporate', AccountType) & (Audience1 == ''|is.na(Audience1)), 'Other Corporate', Audience1),
         Audience1 = ifelse(grepl('Foundation', AccountType), 'Foundation', Audience1),
         Audience1 = ifelse(grepl('Academic', AccountType) & (Account != '' | is.na(Account) | Account != 'Unknown'), 'Academic', Audience1),
         Audience1 = ifelse((is.na(Audience1) | Audience1 == '') & grepl('Foundation', AccountType), 'Foundation', Audience1)) %>% 
  mutate(Account = ifelse(is.na(Account), 'Unknown', Account),
         Audience1 = ifelse(Account == 'Unknown', '', Audience1),
         Audience2 = ifelse(grepl('Gov', Audience1), 'Government', Audience1),
         Account = ifelse(grepl('RMI', Account), 'Household', Account),
         Account = ifelse(Account == 'Unknown' & !is.na(govName), govName, Account),
         Account = ifelse(grepl('Household', Account)|AccountType == 'Household', 'Household', Account),
         Account = ifelse(is.na(Account), 'Unknown', Account),
         DonorType = ifelse(!is.na(Giving_Circle) & Giving_Circle != '', 'Giving Circle',
                            ifelse(!is.na(Last_Gift) & Last_Gift != '', 'Donor', NA)),
         Icon = ifelse(EngagementType == 'Report Download', 1, 
                       ifelse(EngagementType == 'Event', 2, 
                              ifelse(EngagementType == 'Newsletter Click', 3, NA))),
         Pardot_ID = sub("(.*)=", "", Pardot_URL)) %>% 
  distinct(Id, CampaignName, .keep_all = TRUE)

## get donations attributed to campaigns
donors <- df %>% 
  select(Pardot_ID, Name, DonorType, EngagementType, CampaignName, CreatedDate, Id, Pardot_Score, NumGifts, TotalGiving, Account) %>% 
  distinct() %>% 
  filter(!is.na(DonorType))

getDonations <- function(){
  
  allOpportunities <- data.frame(DonationID = '', DonationDate = '', DonationValue = '', Pardot_ID = '', CreatedDate = '',
                                 EngagementType = '', CampaignName = '', Pardot_Score = '',
                                 NumGifts = '', TotalGiving = '', Account = '', Name = '', DonorType = '')[0,]
  for(i in 1:nrow(donors)){
    visitorActivityCall <- GET(paste0('https://pi.pardot.com/api/opportunity/version/4/do/query?format=json&prospect_id=', donors[i, 'Pardot_ID']),
                               add_headers(.headers = header4))
    visitorActivity <- jsonlite::fromJSON(content(visitorActivityCall, as = "text", encoding = "UTF-8"))
    tryCatch( { 
      if(visitorActivity[["result"]][["total_results"]] != 0){
        VA <- data.frame(DonationId = visitorActivity[["result"]][["opportunity"]][["id"]],
                         DonationDate = visitorActivity[["result"]][["opportunity"]][["created_at"]],
                         DonationValue = visitorActivity[["result"]][["opportunity"]][["value"]],
                         DonationStatus = visitorActivity[["result"]][["opportunity"]][["status"]]) %>% 
          filter(DonationStatus == 'Won') %>% 
          cbind(donors[i, 'CreatedDate']) %>% 
          mutate(Pardot_ID = paste(donors[i, 'Pardot_ID']),
                 Id = paste(donors[i, 'Id']),
                 DonationDate = as.Date(DonationDate, format = "%Y-%m-%d"),
                 CreatedDate = as.Date(CreatedDate, format="%Y-%m-%d"),
                 EngagementType = paste(donors[i, 'EngagementType']),
                 CampaignName = paste(donors[i, 'CampaignName']),
                 Pardot_Score = paste(donors[i, 'Pardot_Score']),
                 NumGifts = paste(donors[i, 'NumGifts']),
                 TotalGiving = paste(donors[i, 'TotalGiving']),
                 Account = paste(donors[i, 'Account']),
                 Name = paste(donors[i, 'Name']),
                 DonorType = paste(donors[i, 'DonorType'])) 
        allOpportunities <- allOpportunities %>% rbind(VA)
      }
    }, error = function(e){ NA } )
    
  }
  return(allOpportunities)
}

print('get donations')
allOpps <- getDonations() 

uniqueDonations <- allOpps %>% 
  group_by(DonationId) %>% 
  summarize(count = n())

# select donations made after campaign
donations <- allOpps %>% 
  filter(DonationDate > CreatedDate) %>% 
  # calculate attributed donation value
  mutate(DonationValue = as.numeric(DonationValue),
         difftime = difftime(DonationDate, CreatedDate, units = "days"),
         difftime = as.numeric(gsub("[^0-9.-]", "", difftime)),
         DiffTime = ifelse(difftime < 31, 1, difftime),
         AttributtedValue = ifelse(DonationValue / (1/(1.00273 - 0.00273*DiffTime)) < 0, 0, DonationValue / (1/(1.00273 - 0.00273*DiffTime)))) %>% 
  relocate(DonationValue, .before = AttributtedValue) %>%  
  left_join(uniqueDonations) %>% 
  mutate(AttributtedValue = round(AttributtedValue/count, 1))

oppsByProspect <- donations %>% 
  group_by(Id, CampaignName) %>% 
  summarize(AttributtedDonationValue = sum(AttributtedValue))

# bind donations and clean dataframe
final <- df %>% 
  left_join(oppsByProspect) %>% 
  mutate(DownloadTF = ifelse(grepl('Download', EngagementType), 1, NA),
         EventTF = ifelse(grepl('Event', EngagementType), 1, NA),
         EmailClickTF = ifelse(grepl('Click', EngagementType), 1, NA),
         GivingCircleTF = ifelse(Giving_Circle == ''|is.na(Giving_Circle), NA, 1),
         OpenOppTF = ifelse(NumOpenOpps == ''|NumOpenOpps == 0|is.na(NumOpenOpps), NA, 1),
         DonorTF = ifelse(DonorType == ''|is.na(DonorType), NA, 1),
         LastGift = as.numeric(Last_Gift),
         LapsedDonorsTF = ifelse((LastGift > 549 & LastGift < 1825), 1, NA),
         Engagements = 1) %>% 
  select(CampaignName, EngagementType, Icon, Id, RecordType, Status, EngagementDate = CreatedDate, Name, Title, Domain, Email, 
         LastGift, DonorType, AttributtedDonationValue, AccountId, Account, AccountType, Audience1, Audience2, Industry, 
         AccountTotalGiving = TotalGiving, NumGifts, NumOpenOpps, 
         Pardot_URL, Pardot_ID, GivingCircleTF, OpenOppTF, DonorTF, LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) 

# push data
print('push salesforce data')

write_sheet(final, ss = ss, sheet = 'Salesforce - All')
write_sheet(donations, ss = ss, sheet = 'Salesforce - Donations')


