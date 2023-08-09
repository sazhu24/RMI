
### get packages and functions
source("packages.R")  
source("functions.R")  

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
options(gargle_oauth_cache = ".secrets")
gs4_auth(cache = ".secrets", email = "sazhu24@amherst.edu")

## GA Authentication
ga_auth(email = "sara.zhu@rmi.org")

## SF Authentication
sf_auth()


### SET CAMPAIGN
ss <- 'https://docs.google.com/spreadsheets/d/1GBZl3mUz9DvroeQ77anMtGO4FM0xEnVx4K99Rr6Aw5g/edit#gid=0'
campaign <- 'Coal v Gas'


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
  
} else {
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

## get newsletter stories that match page URLS
campaignNewsletters <- getCampaignEmails(pageURLs)

# set hasEmail to FALSE if no emails detected
if(nrow(campaignNewsletters) == 0) hasEmail <- FALSE else hasEmail <- TRUE

# push data
print('push email stats data')

write_sheet(campaignNewsletters, ss = ss, sheet = 'Newsletter Stats')


#### SALESFORCE
print('GET SALESFORCE DATA')

getSalesforceData()


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

# get all social media posts with tags (excluding posts from linkedIn program channels)
allPostsTags <- getAllSocialPosts()

# get all posts from linkedIn program channels
linkedInTagged <- getLIProgramPosts('tagged')

# clean all posts and bind linkedin program posts
taggedPosts <- cleanPostDF(allPostsTags, 'tagged') %>% 
  rbind(linkedInTagged)

### get brand post metrics (AVGs) based on brand posts made over the last year
posts1YRaverage <- getPostAverages()

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
                         metrics = campaignBoard[11, 'text']) %>% 
  mutate(dashboardCampaign = campaignID)

# push data
print('push monday.com data')

write_sheet(campaignDF, ss = ss, sheet = 'Campaign Overview')


