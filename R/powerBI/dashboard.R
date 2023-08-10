
### get packages and functions
source("/Users/sara/Desktop/GitHub/RMI_Analytics/R/powerBI/packages.R")  
source("/Users/sara/Desktop/GitHub/RMI_Analytics/R/powerBI/functions.R")  

### SET CAMPAIGN

# current options are 1. OCI 2. Coal v Gas
campaign <- 'OCI'

### API Authentication + Tokens

## Monday.com Token
mondayToken <- Sys.getenv("Monday_Token")

## Sprout Social Token
sproutToken <- Sys.getenv("SproutSocial_Token")
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")
currentDate <- paste(Sys.Date())
oneYearAgo <- ymd(currentDate) - years(1)

## Pardot API Token & Request Headers
pardotTokenV4 <- Sys.getenv("Pardot_TokenV4")
pardotTokenV5 <- Sys.getenv("Pardot_TokenV5")
pardotBusinessID <- Sys.getenv("Pardot_Business_ID")
header4 <- c("Authorization" = pardotTokenV4, "Pardot-Business-Unit-Id" = pardotBusinessID)
header5 <- c("Authorization" = pardotTokenV5, "Pardot-Business-Unit-Id" = pardotBusinessID)

## Google Authentication
# options(gargle_oauth_cache = ".secrets")
# gs4_auth(cache = ".secrets", email = "sazhu24@amherst.edu")

## GA Authentication
ga_auth(email = "sara.zhu@rmi.org")

## SF Authentication
sf_auth()

## set google sheet
ss <- 'https://docs.google.com/spreadsheets/d/1FtZQKYp4ESsY5yQzKuvGT5TorKSyMdvRo4bxg6TI7DU/edit?usp=sharing'

## set mode
# standard mode binds data to existing rows
# development mode overwrites all data in sheet
mode <- 'development'

### READ CAMPAIGN KEY
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


##### WEB
print('GET GOOGLE ANALYTICS DATA')

### set GA variables and property ID
rmiPropertyID <- 354053620
metadataGA4 <- ga_meta(version = "data", rmiPropertyID)
dateRangeGA <- c("2023-01-01", paste(currentDate))

### get referral sites
referralSites <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/audiences/referralSites.xlsx') 
  
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

## get acquisition
acquisition <- getAcquisition(rmiPropertyID, pages) 

## get social traffic
socialTraffic <- getTrafficSocial(rmiPropertyID, pages) 

## get geographic segments
geographyTraffic <- getTrafficGeography(rmiPropertyID, pages) 

## get referrals
mediaReferrals <- getReferrals(rmiPropertyID, pages)

### run if new site exists
if(length(propertyIDs) > 1){
  
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
  
  ### get acquisition
  acquisitionNS <- getAcquisition(sitePropertyID, pages, site = newSiteURL) 
  acquisition <- acquisition %>% rbind(acquisitionNS)
  
  # bind page metrics and pivot table so that sessions/conversions are stored in one column
  # this is to make a Power BI table column that changes based on an applied filter
  allTraffic <- pageData %>% 
    select(site, pageTitle) %>% 
    distinct() %>% 
    plyr::rbind.fill(acquisition) %>% 
    pivot_longer(cols = c(Sessions:'Form Submissions'), names_to = "type", values_to = "count") %>% 
    mutate(count = round(count, 1)) %>% 
    left_join(select(pageMetrics, c(pageTitle, screenPageViews:avgEngagementDuration, pageType, icon)), by = 'pageTitle') %>% 
    mutate(totalPageViews = round(screenPageViews/6, 0),
           count = as.numeric(ifelse(is.na(count), 0, count))) %>% 
    filter(defaultChannelGroup != 'Unassigned' & !is.na(defaultChannelGroup)) 
  
  ### get social and bind
  socialTrafficNS <- getTrafficSocial(sitePropertyID, pages, site = newSiteURL) 
  
  socialTraffic <- socialTraffic %>% 
    rbind(socialTrafficNS)
  
  ### get geography and bind
  geographyTrafficNS <- getTrafficGeography(sitePropertyID, pages, site = newSiteURL) 
  
  geographyTraffic <- geographyTraffic %>% 
    rbind(geographyTrafficNS)
  
  ### get referrals and bind
  mediaReferralsNS <- getReferrals(sitePropertyID, pages, site = newSiteURL)
  
  mediaReferrals <- mediaReferrals %>% 
    rbind(mediaReferralsNS)
  
} else {
  # bind page metrics and pivot table so that sessions/conversions are stored in one column
  # this is to make a Power BI table column that changes based on an applied filter
  allTraffic <- pageData %>% 
    select(site, pageTitle) %>% 
    distinct() %>% 
    plyr::rbind.fill(acquisition) %>% 
    pivot_longer(cols = c(Sessions:'Form Submissions'), names_to = "type", values_to = "count") %>% 
    mutate(count = round(count, 1)) %>% 
    left_join(select(pageMetrics, c(pageTitle, screenPageViews:avgEngagementDuration, pageType, icon)), by = 'pageTitle') %>% 
    mutate(totalPageViews = round(screenPageViews/6, 0),
           count = as.numeric(ifelse(is.na(count), 0, count))) %>% 
    filter(defaultChannelGroup != 'Unassigned' & !is.na(defaultChannelGroup)) 
}


## push google analytics data

ALL_WEB_TRAFFIC <- pushData(allTraffic, 'Web Traffic - All')
ALL_WEB_SOCIAL <- pushData(socialTraffic, 'Web Traffic - Social')
ALL_WEB_GEO <- pushData(geographyTraffic, 'Web Traffic - Geography')
ALL_WEB_REFERRALS <- pushData(mediaReferrals, 'Web Traffic - Referrals')


##### EMAIL NEWSLETTERS 
print('GET EMAIL DATA')

## get all email stats 
allEmailStats <- getAllEmailStats()

## get newsletter story URLs
pageURLs <- pageData$pageURL

## get newsletter stories that match page URLS
campaignNewsletters <- getCampaignEmails(pageURLs)

# set hasEmail to FALSE if no emails detected
if(nrow(campaignNewsletters) == 0) hasEmail <- FALSE else hasEmail <- TRUE

## push data
print('push email stats data')

ALL_NEWSLETTERS <- pushData(campaignNewsletters, 'Newsletters')


#### SALESFORCE
print('GET SALESFORCE DATA')

if(hasReport == TRUE | hasEvent == TRUE | hasEmail == TRUE){
  
  ## get list of campaigns
  campaignList <- getCampaignList()
  
  ## get all accounts
  all_accounts <- getAllAccounts() 
  
  ## remove accounts with duplicate names to avoid errors when joining by Account name
  accountsUnique <- all_accounts[!duplicated(all_accounts$Account) & !duplicated(all_accounts$Account, fromLast = TRUE),] %>% 
    filter(!grepl('unknown|not provided|contacts created by revenue grid', Account))
  
  ## get domain info for gov accounts
  govDomains <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/audiences/govDomains.xlsx') 
  
  ## get audience domains and accounts
  audienceGroups <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/audiences/audienceGroups.xlsx')
  audienceAccounts <- audienceGroups %>% select(Account, type) %>% filter(!is.na(Account)) %>% distinct(Account, .keep_all = TRUE)
  audienceDomains <- audienceGroups %>% select(Domain, type) %>% filter(!is.na(Domain)) %>% distinct(Domain, .keep_all = TRUE)
}

final <- getSalesforceData()
donations <- getDonationsDF(final)

# bind donations to SF campaign data
oppsByProspect <- donations %>% 
  group_by(Id, CampaignName) %>% 
  summarize(AttributtedDonationValue = sum(AttributtedValue))

final <- final %>% 
  left_join(oppsByProspect) %>% 
  select(CampaignName, EngagementType, Icon, Id, Status, EngagementDate, Domain, Email, 
         DonorType, AttributtedDonationValue, AccountId, Account, AccountType, Audience1, Audience2, Industry, 
         Pardot_URL, Pardot_ID, GivingCircleTF, SolutionsCouncilTF, InnovatorsCircleTF, OpenOppTF, DonorTF, 
         LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) 

# push data
print('push salesforce data')

ALL_SALESFORCE <- pushData(final, 'Salesforce')
ALL_DONATIONS <- pushData(donations, 'SF Donations')



##### SOCIAL MEDIA
print('GET SOCIAL MEDIA DATA')

# get profile IDs
# profile IDs are specific to an account (e.g. RMI Brand LinkedIn, RMI Buildings Twitter)
# supply the appropriate profile ID for each account you want to include in your request
metadeta <- getMetadata(url = 'metadata/customer')
profileIDs <- metadeta[["data"]]

# get all tags
metadeta <- getMetadata(url = 'metadata/customer/tags')
tags <- metadeta[["data"]]

# find the appropriate campaign tag ID using the socialTag
campaignTag <- tags %>% filter(text == socialTag)
tagID <- paste(campaignTag$tag_id)

# get all social media posts with tags (excluding posts from linkedIn program channels)
allPostsTags <- getAllSocialPosts()

# get all posts from linkedIn program channels
linkedInTagged <- getLIProgramPosts('tagged')

# clean and bind linkedin program posts to all posts
taggedPosts <- cleanPostDF(allPostsTags, 'tagged') %>% 
  rbind(linkedInTagged)

# get post metrics (AVGs) based on posts made over the last year (for brand accounts only)
posts1YRaverage <- getPostAverages()

###

# find tagged posts by applying filter to all posts 
campaignPosts <- taggedPosts %>% 
  filter(id == tagID) %>% 
  #filter(created_time >= '2023-04-05' & grepl('OCI\\+', text)) %>% 
  distinct(perma_link, .keep_all = TRUE) %>% 
  # calculate post performance compared to avgs.
  left_join(posts1YRaverage, by = c('post_type', 'account')) %>% 
  mutate(impressionsVavg= round((impressions - impressionsAVG)/impressionsAVG, 3),
         engrtVavg = round((engagementRate - engrtAVG)/engrtAVG, 3),
         brand = ifelse(grepl('RMI Brand', account), 1, 0),
         program = ifelse(brand == 1, 0, 1),
         accountType = ifelse(brand == 1, 'Brand', 'Program'),
         post = 'Post') %>% 
  select(-c(impressionsAVG, engagementsAVG, engrtAVG)) %>% 
  relocate(perma_link, .after = post) %>% 
  relocate(text, .after = perma_link) 

# push data
print('push social media data')

ALL_SOCIAL_POSTS <- pushData(campaignPosts, 'Social Media Posts')

#### Monday.com

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

# filter to find campaign
targetCampaign <- campaigns %>% 
  filter(grepl('Coal v Gas', name))

# get metrics, audiences, and ID
campaignRow <- as.numeric(targetCampaign[1, 'row'])
campaignBoard <- activeProjects[[3]][[campaignRow]]
campaignDF <- data.frame(ID = campaignBoard[16, 'text'], 
                         audiences = campaignBoard[15, 'text'], 
                         metrics = campaignBoard[11, 'text']) 

# push data
print('push monday.com data')

ALL_MONDAY <- pushData(campaignDF, 'Campaign Overview')


##### CREATE CONTENT SUMMARY #####

socialContent <- campaignPosts %>% 
  mutate(type = 'Social Media Posts') %>% 
  select(type, name = post_type) 

salesforceContent <- final %>% 
  select(name = CampaignName, EngagementType) %>% 
  distinct() %>% 
  mutate(type = ifelse(EngagementType == 'Newsletter', 'Newsletters', 
                       ifelse(EngagementType == 'Report Download', 'Reports', 
                              ifelse(EngagementType == 'Event', 'Events', '')))) %>% 
  select(type, name)

mediaContent <- mediaReferrals %>% 
  mutate(type = 'Media Referrals') %>% 
  ungroup() %>% 
  select(type, name = mediaSubtype) 

contentSummary <- socialContent %>% 
  rbind(salesforceContent) %>% 
  rbind(mediaContent)

ALL_CONTENT_SUMMARY <- pushData(contentSummary, 'Content Summary')


