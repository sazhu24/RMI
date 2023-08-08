library(dplyr)
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
library(conflicted)
library(stringr)

conflicts_prefer(jsonlite::toJSON)
conflicts_prefer(jsonlite::fromJSON)
conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::mutate)
conflicts_prefer(dplyr::group_by)
conflicts_prefer(dplyr::summarize)
conflicts_prefer(dplyr::rename)

### API Authentication + Tokens

# Monday.com Token
mondayToken <- Sys.getenv("Monday_Token")

# Sprout Social Token
sproutToken <- Sys.getenv("SproutSocial_Token")
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")
currentDate <- paste(Sys.Date())
yearAgo <- ymd(currentDate) - years(1)

# Pardot API Token & Request Headers
pardotTokenV4 <- Sys.getenv("Pardot_TokenV4")
pardotTokenV5 <- Sys.getenv("Pardot_TokenV5")
pardotBusinessID <- Sys.getenv("Pardot_Business_ID")

header4 <- c("Authorization" = pardotTokenV4, "Pardot-Business-Unit-Id" = pardotBusinessID)
header5 <- c("Authorization" = pardotTokenV5, "Pardot-Business-Unit-Id" = pardotBusinessID)

## Google Authentication
options(gargle_oauth_cache = ".secrets")
gs4_auth(cache = ".secrets", email = "sazhu24@amherst.edu")

## GA Authentication
ga_auth(email = "sara.zhu@rmi.org")


### CAMPAIGN
campaign <- 'OCI'

## get campaign key
campaignKey <- read_sheet('https://docs.google.com/spreadsheets/d/1YyF4N2C9En55bqzisSi8TwUMzsvMnEc0jgFYdbBt3O0/edit?usp=sharing', 
                          sheet = paste0('Campaign Key - ', campaign))

campaignID <- as.character(campaignKey[1, c('campaignID')])
campaignPages <- campaignKey[, c('propertyID', 'page')]

propertyIDs <- unique(campaignKey$propertyID)
propertyIDs <- propertyIDs[!is.na(propertyIDs)]

campaignReports <- campaignKey[!is.na(campaignKey$reportID), 'reportID']
campaignEvents <- campaignKey[!is.na(campaignKey$eventID), 'eventID']

if(nrow(campaignReports) == 0) hasReport <- FALSE else hasReport <- TRUE
if(nrow(campaignEvents) == 0) hasEvent <- FALSE else hasEvent <- TRUE

socialTag <- as.character(campaignKey[1, c('socialTag')])

### get functions
source("functions.R")  

##### WEB
print('GET GOOGLE ANALYTICS DATA')

### set GA variables and property ID
rmiPropertyID <- 354053620
metadataGA4 <- ga_meta(version = "data", rmiPropertyID)
dateRangeGA <- c("2023-01-01", paste(currentDate))

### get referral sites
referralSites <- read_sheet('https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843', 
                            sheet = 'All Referral Sites')

###
campaignPages <- campaignPages %>% 
  mutate(site = ifelse(propertyID == rmiPropertyID, 'rmi.org', sub('/(.*)', '', sub('(.*)https://', '', page)))) %>% 
  filter(!is.na(propertyID))

pageData <- data.frame(site = campaignPages$site, 
                       pageURL = campaignPages$page, 
                       pageTitle = '', 
                       pageType = '', 
                       metadata = '')

pageData <- getPageData(pageData)

## set page titles
rmiPages <- pageData %>% filter(site == 'rmi.org')
pages <- rmiPages[['pageTitle']]

## get page metrics
pageMetrics <- getPageMetrics(rmiPropertyID, pages) 
totalPageViews <- sum(pageMetrics$screenPageViews)

## get acquisition
acquisition <- getAcquisition(rmiPropertyID, pages) 

## get social traffic
socialTraffic <- getTrafficSocial(rmiPropertyID, pages) 

## get geographic segments
geographyTraffic <- getTrafficGeography(rmiPropertyID, pages) 

## get referrals
mediaReferrals <- getReferrals(rmiPropertyID, pages)

### run if new site exists
if(length(propertyIDs > 1)){
  
  # set property ID for new website
  sitePropertyID <- propertyIDs[2]
  
  # get pages
  newSitePages <- pageData %>% filter(site != 'rmi.org')
  pages <- unique(newSitePages[['pageTitle']])
  
  # get website URL
  newSiteURL <- unique(newSitePages[['site']])
  
  ### get page metrics + acquisition
  pageMetricsNS <- getPageMetrics(sitePropertyID, pages) %>% 
    distinct()
  
  pageMetrics <- pageMetrics %>% rbind(pageMetricsNS)
  totalPageViews <- sum(pageMetrics$screenPageViews)
  
  ### get acquisition
  acquisitionNS <- getAcquisition(sitePropertyID, pages, site = newSiteURL) 
  acquisition <- acquisition %>% rbind(acquisitionNS)
  
  # bind page metrics and pivot table
  allTraffic <- pageData %>% 
    select(site, pageTitle) %>% 
    distinct() %>% 
    plyr::rbind.fill(acquisition) %>% 
    pivot_longer(cols = c(Sessions:'Form Submissions'), names_to = "type", values_to = "count") %>% 
    mutate(count = round(count, 1)) %>% 
    left_join(select(pageMetrics, c(pageTitle, screenPageViews:avgEngagementDuration, pageType, icon)), by = 'pageTitle') %>% 
    mutate(totalPageViews = totalPageViews,
           dashboardCampaign = campaignID) %>% 
    filter(defaultChannelGroup != 'Unassigned' & !is.na(defaultChannelGroup)) %>% 
    filter(!is.na(count))
  
  ### get social
  socialTrafficNS <- getTrafficSocial(sitePropertyID, pages, site = newSiteURL) 
  
  # bind
  socialTraffic <- socialTraffic %>% 
    rbind(socialTrafficNS)
  
  ### get geography
  geographyTrafficNS <- getTrafficGeography(sitePropertyID, pages, site = newSiteURL) 
  
  # bind
  geographyTraffic <- geographyTraffic %>% 
    rbind(geographyTrafficNS)
  
  ### get referrals
  mediaReferralsNS <- getReferrals(sitePropertyID, pages, site = newSiteURL)
  
  mediaReferrals <- mediaReferrals %>% 
    rbind(mediaReferralsNS)
  
}

allTraffic <- allTraffic %>% 
  mutate(dashboardCampaign = campaignID)

socialTraffic <- socialTraffic %>% 
  mutate(dashboardCampaign = campaignID)

geographyTraffic <- geographyTraffic %>% 
  mutate(dashboardCampaign = campaignID)

mediaReferrals <- mediaReferrals %>% 
  mutate(dashboardCampaign = campaignID)

# push google analytics data
print('push google analytics data')

write_sheet(allTraffic, ss = ss, sheet = 'Web Traffic - All')
write_sheet(socialTraffic, ss = ss, sheet = 'Web Traffic - Social')
write_sheet(geographyTraffic, ss = ss, sheet = 'Web Traffic - Geography')
write_sheet(mediaReferrals, ss = ss, sheet = 'Web Traffic - Referrals')


##### EMAIL NEWSLETTERS 
print('GET EMAIL DATA')

## get all email stats 
allEmailStats <- getAllEmailStats()

## get newsletter story URLs
pageURLs <- pageData$pageURL

# set hasEmail to FALSE if no emails detected
if(nrow(allStoryStats) == 0) hasEmail <- FALSE else hasEmail <- TRUE

# push data
print('push email stats data')

write_sheet(allStoryStats, ss = ss, sheet = 'Campaign Email Stats')


### SALESFORCE

print('GET SALESFORCE DATA')

### run if at least 1 campaign exists

if(hasReport == TRUE | hasEvent == TRUE | hasEmail == TRUE){
  
  ## get list of campaigns
  campaignList <- getCampaignList()
  
  ## get all accounts
  all_accounts <- getAllAccounts() 
  
  ## get domain info for gov accounts
  domainKey <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/domainKey.xlsx') 
  
  ## get audience domains and accounts
  audienceGroups <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/audienceGroups.xlsx')
  audienceAccounts <- audienceGroups %>% select(Account, type) %>% filter(!is.na(Account)) %>% distinct(Account, .keep_all = TRUE)
  audienceDomains <- audienceGroups %>% select(Domain, type) %>% filter(!is.na(Domain)) %>% distinct(Domain, .keep_all = TRUE)
  
  ## create dataframe for campaigns
  df <- as.data.frame(matrix(0, ncol = 19, nrow = 0))
  names(df) <- c('CampaignName', 'EngagementType', 'Id', 'RecordType', 'Status', 'EngagementDate', 
                 'Name', 'Email', 'Domain', 'Account', 'AccountType', 'Industry', 'TotalGiving', 
                 'NumOpenOpps', 'Pardot_Score', 'Pardot_URL', 'Giving_Circle', 'Last_Gift', 'AccountId') 
  
  if(hasReport == TRUE){
    
    print('get reports')
    campaignMembersReports <- getCampaignMembers(campaignReports$reportID, 'Report') 
    campaignMembersReports <- cleanCampaignDF(campaignMembersReports)
    
    df <- df %>% 
      rbind(campaignMembersReports)
  }
  
  if(hasEvent == TRUE){
    
    print('get events')
    campaignMembersEvents <- getCampaignMembers(campaignEvents$eventID, 'Event')
    campaignMembersEvents <- cleanCampaignDF(campaignMembersEvents)
    
    df <- df %>% 
      rbind(campaignMembersEvents)
  }
  
  ##### GET EMAIL CLICKS
  
  if(hasEmail == TRUE){
    
    print('get emails')
    
    ## get all prospects
    prospects <- getProspects() %>% 
      mutate(Domain = sub("(.*)\\@", "", Email))
    
    ## get all email names/info
    allEmailStats <- getAllEmailStats()
    
    ## get email IDs and URLs from email story stats data
    emailIDs <- data.frame(id = unique(allStoryStats$id))
    storyLinks <- unique(allStoryStats$story_url)
    
    ## get clicks for email of interest
    print('get email clicks')
    clicksAll <- getProspectClicks(emailIDs)
    
    ## clean and filter links
    linkClicks <- clicksAll %>% 
      mutate(url = sub('\\?(.*)', '', url)) %>% 
      filter(grepl(paste(storyLinks, collapse = '|'), url))
    
    accountsUnique <- all_accounts[!duplicated(all_accounts$Account) & !duplicated(all_accounts$Account, fromLast = TRUE),] %>% 
      filter(!grepl('unknown|not provided|contacts created by revenue grid', Account))
    
    # clean clicks df
    clicksByProspect <- select(linkClicks, c(emailId = list_email_id, Pardot_URL = prospect_id, url, created_at)) %>% 
      mutate(Pardot_URL = paste0("http://pi.pardot.com/prospect/read?id=", as.character(Pardot_URL)),
             EngagementDate = as.Date(created_at, format="%Y-%m-%d")) %>% 
      left_join(prospects, by = c('Pardot_URL')) %>% 
      left_join(select(allEmailStats, c(emailId = id, CampaignName = name)), by = 'emailId') %>% 
      mutate(Status = 'Email',
             EngagementType = 'Newsletter Click') %>% 
      mutate(Account = ifelse(grepl('unknown|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account)) %>% 
      left_join(select(accountsUnique, c(AccountsName = Account, AccountDomain, Industry, AccountType, TotalGiving, 
                                         NumGifts, NumOpenOpps, AccountId2 = AccountId)), 
                by = c('Account' = 'AccountsName')) %>% 
      select(CampaignName, EngagementType, Id, RecordType, Status, EngagementDate, Name, Email, Domain, Account, AccountType, Industry,
             TotalGiving, NumOpenOpps, Pardot_Score, Pardot_URL, Giving_Circle, Last_Gift, AccountId)
    
    campaignMembersEmail <- cleanCampaignDF(clicksByProspect)
    
    df <- df %>% 
      rbind(campaignMembersEmail)
  }
  
  df <- df %>% distinct(Id, CampaignName, .keep_all = TRUE)
  
  ## get donations attributed to campaigns
  print('get donations')
  
  donors <- df %>% 
    select(Pardot_ID, Name, DonorType, EngagementType, CampaignName, EngagementDate, Id, Pardot_Score, TotalGiving, Account) %>% 
    distinct() %>% 
    filter(!is.na(DonorType))
  
  print('get donations')
  allOpps <- getDonations() 
  
  # select donations made after campaign
  donations <- allOpps %>% 
    filter(DonationDate > EngagementDate)
  
  uniqueDonations <- donations %>% 
    dplyr::group_by(DonationId) %>% 
    dplyr::summarize(count = n())
  
  donations <- donations %>% 
    # calculate attributed donation value
    mutate(DonationValue = as.numeric(DonationValue),
           TimeDifference = difftime(DonationDate, EngagementDate, units = "days"),
           TimeDifference = as.numeric(gsub("[^0-9.-]", "", TimeDifference)),
           TimeDifferenceAdjusted = ifelse(TimeDifference < 31, 1, TimeDifference),
           AttributtedValue = ifelse(DonationValue / (1/(1.0041 - 0.0041*TimeDifferenceAdjusted)) < 0, 0, DonationValue / (1/(1.0041 - 0.0041*TimeDifferenceAdjusted)))) %>% 
    relocate(DonationValue, .before = AttributtedValue) %>%  
    left_join(uniqueDonations) %>% 
    mutate(AttributtedValue = round(AttributtedValue/count, 1)) %>% 
    select(-TimeDifferenceAdjusted) %>% 
    mutate(dashboardCampaign = campaignID)
  
  oppsByProspect <- donations %>% 
    group_by(Id, CampaignName) %>% 
    summarize(AttributtedDonationValue = sum(AttributtedValue))
  
  # bind donations and clean dataframe
  final <- df %>% 
    left_join(oppsByProspect) %>% 
    select(CampaignName, EngagementType, Icon, Id, Status, EngagementDate, Domain, Email, 
           DonorType, AttributtedDonationValue, AccountId, Account, AccountType, Audience1, Audience2, Industry, 
           Pardot_URL, Pardot_ID, GivingCircleTF, SolutionsCouncilTF, InnovatorsCircleTF, OpenOppTF, DonorTF, 
           LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) %>% 
    # bind campaign ID
    mutate(dashboardCampaign = campaignID)
  
  # push data
  print('push salesforce data')
  
  write_sheet(final, ss = ss, sheet = 'Salesforce - All')
  write_sheet(donations, ss = ss, sheet = 'Salesforce - Donations')
  
} else {
  print('there are no reports, events, or emails in this campaaign')
}


##### SOCIAL MEDIA
print('GET SOCIAL MEDIA DATA')

# get profile IDs
metadeta <- getMetadata(url = 'metadata/customer')
profileIDs <- metadeta[["data"]]

# get all tags
metadeta <- getMetadata(url = 'metadata/customer/tags')
tags <- metadeta[["data"]]

# find campaign tag
campaignTag <- tags %>% filter(text == socialTag)
tagID <- paste(campaignTag$tag_id)

# create data frame to store all posts
allPostsTags <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', 
                           lifetime.impressions = '', lifetime.post_content_clicks = '', 
                           lifetime.engagements = '', lifetime.shares_count = '', id = '')[0, ]

# get all posts from social channels dating back to Jan 1, 2023
for(i in 1:300){
  postStats <- sproutPostRequest(i, 
                                 dateRange = paste0("created_time.in(", '2023-01-01', "T00:00:00..", currentDate, "T23:59:59)"),
                                 profileIDs = '(3244287, 2528134, 2528107, 2528104, 3354378, 4145669, 4400432, 4613890, 5083459, 5097954, 5098045, 5251820, 5334423, 5403312, 3246632, 5403593, 5403597)') 
  if(is.null(postStats)){ break }
  
  # unnest tags
  postStats <- postStats %>% unnest(tags)
  # bind query to data frame
  allPostsTags <- allPostsTags %>% rbind(postStats)
}

# get all posts from linkedIn program channels
linkedInTagged <- getLIProgramPosts('tagged')

# clean all posts and bind linkedin program posts
taggedPosts <- cleanPostDF(allPostsTags, 'tagged') %>% 
  rbind(linkedInTagged)

### get averages

# create data frames
posts1YR <- data.frame(created_time = '', post_type = '', text = '', perma_link = '',
                       lifetime.impressions = '', lifetime.post_content_clicks = '',
                       lifetime.engagements = '', lifetime.shares_count = '')[0, ]

# get all posts from social channels over the past year
for(i in 1:300){
  postStats <- sproutPostRequest(i, 
                                 dateRange = paste0("created_time.in(", oneYearAgo, "T00:00:00..", currentDate, "T23:59:59)"),
                                 profileIDs = '(2528104, 2528107, 2528134, 3244287)',
                                 AVG = TRUE)
  if(is.null(postStats)){ break }
  
  posts1YR <- posts1YR %>% rbind(postStats)
}

posts1YRaverage <- cleanPostDF(posts1YR, 'all') %>%
  filter(impressions > 0 & !grepl('ugcPost', perma_link)) %>%
  group_by(post_type, account) %>%
  summarize(
    impressionsAVG = round(mean(impressions, na.rm = TRUE), 1),
    engagementsAVG = round(mean(engagements), 1),
    engrtAVG = round(mean(engagementRate), 3))


###

campaignPosts <- taggedPosts %>% 
  filter(id == tagID) %>% 
  #filter(created_time >= '2023-04-05' & grepl('OCI\\+', text)) %>% 
  distinct(perma_link, .keep_all = TRUE) %>% 
  left_join(posts1YRaverage, by = c('post_type', 'account')) %>% 
  mutate(impressionsVavg= round((impressions - impressionsAVG)/impressionsAVG, 3),
         engrtVavg = round((engagementRate - engrtAVG)/engrtAVG, 3),
         brand = ifelse(grepl('RMI Brand', account), 1, 0),
         program = ifelse(brand == 1, 0, 1),
         accountType = ifelse(brand == 1, 'Brand', 'Program'),
         post = 'Post') %>% 
  select(-c(impressionsAVG, engagementsAVG, engrtAVG)) %>% 
  relocate(perma_link, .after = post) %>% 
  relocate(text, .after = perma_link) %>% 
  mutate(dashboardCampaign = campaignID)

# push data
print('push social media data')

write_sheet(campaignPosts, ss = ss, sheet = 'Social - Campaign')


##### Monday.com
print('GET MONDAY.COM DATA')

# get Active Projects Board
query <- "query { boards (ids: 2208962537) { items { id name column_values{ id value text } } } } "
res <- getMondayCall(query)
activeProjects <- as.data.frame(res[["data"]][["boards"]][["items"]][[1]])

# iterate through APB to find projects with "Metrics Dashboard" in the promotion tactics column
print('find Metrics Dashboard projects')

campaigns <- data.frame(id = '', row = '', name = '')[0,]
for(i in 1:nrow(activeProjects)){
  
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
  filter(grepl('Coal v Gas', name))

# get metrics, audiences, and ID
campaignRow <- as.numeric(targetCampaign[1, 'row'])
campaignBoard <- activeProjects[[3]][[campaignRow]]
campaignDF <- data.frame(ID = campaignBoard[16, 'text'], 
                         audiences = campaignBoard[15, 'text'], 
                         metrics = campaignBoard[11, 'text']) %>% 
  mutate(dashboardCampaign = campaignID)

# push data
print('push monday.com data')

write_sheet(campaignDF, ss = ss, sheet = 'Campaign Overview')


