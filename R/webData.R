library(tidyverse)
library(openxlsx)
library(googledrive)
library(googlesheets4)
library(googleAnalyticsR)
library(rvest)
library(rjson)

## Google Analytics
ga_auth(email = "sara.zhu@rmi.org")
property_id <- 354053620
metadataGA4 <- ga_meta(version = "data", property_id)
currentDate <- Sys.Date()
dates1 <- c("2023-01-01", paste(currentDate))

## push to this google sheet
ss <- 'https://docs.google.com/spreadsheets/d/1_wUKRziRhF90ZfUemHNt3DaOjU2fQlTIPO8rNlA9doY/edit#gid=1033361432' ## OCI
ss <- 'https://docs.google.com/spreadsheets/d/1qkLvjHl8FTRZ4t4OenI6QcCtG8zkxz1VHppn3JuWjYE/edit#gid=1178168276' ## Coal vs. Gas

### look for content group tag - system not set up yet
contentGroup <- ga_data(
  property_id,
  metrics = c("engagedSessions", "sessions", "engagementRate", "averageSessionDuration", "screenPageViews", "newUsers", "totalUsers"),
  dimensions = c("contentGroup", 'pageTitle', 'pagePath'),
  date_range = dates1,
  limit = -1
) %>% 
  filter(contentGroup != "(not set)") %>% 
  filter(pageTitle != '')

groupPages <- contentGroup$pageTitle

### OCI+ Campaign
pageTitles <- c('Top Strategies to Cut Dangerous Methane Emissions from Landfills - RMI',
                'OCI+ Update: Tackling Methane in the Oil and Gas Sector - RMI',
                'Clean Energy 101: Methane-Detecting Satellites - RMI',
                'Waste Methane 101: Driving Emissions Reductions from Landfills - RMI',
                'Key Strategies for Mitigating Methane Emissions from Municipal Solid Waste - RMI',
                'Know Your Oil and Gas - RMI',
                'Intel from Above: Spotting Methane Super-Emitters with Satellites - RMI')

### Get Full URLs 
getLinks <- ga_data(
  property_id,
  metrics = c("sessions", "totalUsers", "engagementRate", "userEngagementDuration", "screenPageViews"),
  dimensions = c("pageTitle", 'fullPageUrl'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pageTitles),
  limit = -1
) %>% 
  mutate(fullPageUrl = paste0('https://', fullPageUrl))

pages <- getLinks$pageTitle
linkTitles <- getLinks$pageTitle
linkUrls <- getLinks$fullPageUrl

### get page type by scraping website metadata for each page
pageData <- data.frame(linkTitles, linkUrls, metadata1 = '', metadata2 = '')
for(i in 1:nrow(pageData)){

  url <- paste(pageData[i, 'linkUrls'])
  tryCatch( { 
    #open connection to url 
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


### get web traffic and key metrics for all pages
campaignPages <- ga_data(
  property_id,
  metrics = c('screenPageViews', "totalUsers", "userEngagementDuration", 'conversions', 'conversions:form_submit', 'conversions:file_download', 'conversions:click'),
  dimensions = c("pageTitle"),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  mutate(engagementDuration = userEngagementDuration / totalUsers) %>% 
  relocate(engagementDuration, .after = totalUsers) %>% 
  select(-userEngagementDuration) %>% 
  left_join(select(pageType, c(pageTitle = linkTitles, pageType)), by = c('pageTitle')) %>% 
  mutate(sec = round(engagementDuration %% 60, 0),
         min = (engagementDuration / 60) |> floor(),
         avgEngagementDuration = paste0(min, ':', sec)) %>% 
  select(-sec, -min)

pages <- campaignPages$pageTitle # get list of page titles

campaignPages <- campaignPages %>% # clean end of title
  mutate(pageTitle = gsub(' - RMI', '', pageTitle))

write_sheet(campaignPages, ss = ss, sheet = 'Web - All Pages')

### get page views by region
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

write_sheet(trafficByRegion, ss = ss, sheet = 'Web - Region')

### get aquisition - sessions
aquisitionSessions <- ga_data(
  property_id,
  metrics = c("sessions"),
  dimensions = c("pageTitle", "sessionSource", "sessionMedium", "pageReferrer", 'sessionDefaultChannelGroup'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  arrange(pageTitle) %>% 
  # correct social media and email channel traffic
  mutate(sessionMedium = ifelse(grepl('mail.google.com', sessionSource)|grepl('web-email|sf|outlook', sessionMedium), 
                                'email', sessionMedium),
         sessionSource = ifelse(grepl('linkedin', sessionSource), 'linkedin', sessionSource),
         sessionSource = ifelse(grepl('facebook', sessionSource), 'facebook', sessionSource),
         sessionSource = ifelse(grepl('dlvr.it|twitter', sessionSource)|sessionSource == 't.co', 'twitter', sessionSource),
         sessionMedium = ifelse(grepl('linkedin|lnkd.in|facebook|twitter|instagram', sessionSource)|grepl('twitter|fbdvby', sessionMedium), 'social', sessionMedium)) %>% 
  mutate(pageReferrer = ifelse(grepl('rmi.org', pageReferrer), 'https://rmi.org/', pageReferrer),
         sessionMedium = ifelse(grepl('/t.co/', pageReferrer), 'social', sessionMedium),
         sessionMedium = ifelse(grepl('not set|none', sessionMedium)|sessionMedium == '', 'none', sessionMedium),
         sessionDefaultChannelGroup = ifelse(sessionMedium == 'social', 'Organic Social', 
                                             #ifelse(pageReferrer == 'https://rmi.org/' & sessionMedium == 'none'|sessionMedium == 'direct'|sessionMedium == 'organic', 'rmi.org',
                                                    ifelse(sessionMedium == 'email', 'Email', sessionDefaultChannelGroup))) %>% 
  group_by(pageTitle, sessionDefaultChannelGroup) %>% 
  summarize(sessions = sum(sessions)) %>% 
  rename(defaultChannelGroup = sessionDefaultChannelGroup) %>% 
  mutate(pageTitle = gsub(' - RMI', '', pageTitle))

### get aquisition - conversions
aquisitionConversions <- ga_data(
  property_id,
  metrics = c("conversions", 'conversions:form_submit', 'conversions:file_download', 'conversions:click'),
  dimensions = c("pageTitle", "source", "medium", 'pageReferrer', 'defaultChannelGroup'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  select(pageTitle, source, medium, conversions, pageReferrer, defaultChannelGroup, form_submit = 'conversions:form_submit', download = 'conversions:file_download', click = 'conversions:click') %>% 
  arrange(pageTitle) %>% 
  # correct social media and email channel traffic
  mutate(medium = ifelse(grepl('mail.google.com', source)|grepl('web-email|sf|outlook', medium), 
                                'email', medium),
         source = ifelse(grepl('linkedin', source), 'linkedin', source),
         source = ifelse(grepl('facebook', source), 'facebook', source),
         source = ifelse(grepl('dlvr.it|twitter', source)|source == 't.co', 'twitter', source),
         medium = ifelse(grepl('linkedin|lnkd.in|facebook|twitter|instagram', source)|grepl('twitter|fbdvby', medium), 'social', medium),
         medium = ifelse(grepl('/t.co/', pageReferrer), 'social', medium),
         defaultChannelGroup = ifelse(medium == 'social', 'Organic Social', 
                                      ifelse(medium == 'email', 'Email', defaultChannelGroup))) %>% 
  group_by(pageTitle, defaultChannelGroup) %>% 
  summarize(conversions = sum(conversions),
            downloads = sum(form_submit),
            form_submits = sum(download),
            clicks = sum(click)) %>% 
  mutate(pageTitle = gsub(' - RMI', '', pageTitle))

aquisitionAll <- aquisitionSessions %>% # bind conversions to sessions
  left_join(aquisitionConversions, by = c('pageTitle', 'defaultChannelGroup'))

## acquisition - social
aquisitionSocial <- ga_data(
  property_id,
  metrics = c("sessions", "screenPageViews"),
  dimensions = c("pageTitle", "sessionSource", "sessionMedium", 'pageReferrer'),
  date_range = dates1,
  dim_filters = ga_data_filter("pageTitle" == pages),
  limit = -1
) %>% 
  arrange(pageTitle) %>% 
  # correct social media and email channel traffic
  mutate(sessionMedium = ifelse(grepl('mail.google.com', sessionSource)|grepl('web-email|sf|outlook', sessionMedium), 
                                'email', sessionMedium),
         sessionSource = ifelse(grepl('linkedin', sessionSource), 'linkedin', sessionSource),
         sessionSource = ifelse(grepl('facebook', sessionSource), 'facebook', sessionSource),
         sessionSource = ifelse(grepl('dlvr.it|twitter', sessionSource)|sessionSource == 't.co', 'twitter', sessionSource),
         sessionMedium = ifelse(grepl('linkedin|lnkd.in|facebook|twitter|instagram', sessionSource)|grepl('twitter|fbdvby', sessionMedium), 
                                'social', sessionMedium),
         sessionMedium = ifelse(grepl('/t.co/', pageReferrer), 'social', sessionMedium),
         sessionSource = ifelse(grepl('/t.co/', pageReferrer), 'twitter', sessionSource)) %>% 
  filter(sessionMedium == 'social') %>% 
  group_by(pageTitle, sessionSource) %>% 
  summarize(sessions = sum(sessions),
            screenPageViews = sum(screenPageViews)) %>% 
  mutate(pageTitle = gsub(' - RMI', '', pageTitle))

write_sheet(aquisitionAll, ss = ss, sheet = 'Web - Acquisition')
write_sheet(aquisitionSocial, ss = ss, sheet = 'Web - Social Traffic')

## get email stats and push to dataset

getStoryStats <- function(campaign){
  # connect to link from Email Stats spreadsheet
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
  write_sheet(df, ss = ss, sheet = 'All Email Stats')
  write_sheet(allStoryStats, ss = ss, sheet = 'Campaign Email Stats')

}

getStoryStats()
