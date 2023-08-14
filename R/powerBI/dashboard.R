
#### GET PACKAGES AND FUNCTIONS #### 
source("/Users/sara/Desktop/GitHub/RMI_Analytics/R/powerBI/packages.R")  
source("/Users/sara/Desktop/GitHub/RMI_Analytics/R/powerBI/functions.R")  

#### API TOKENS & AUTHENTICATION ####

#' Monday.com Token
mondayToken <- Sys.getenv("Monday_Token")

#' Sprout Social Token
sproutToken <- Sys.getenv("SproutSocial_Token")
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")
currentDate <- paste(Sys.Date())
oneYearAgo <- ymd(currentDate) - years(1)

#' Pardot API Token & Request Headers
pardotTokenV4 <- Sys.getenv("Pardot_TokenV4")
pardotTokenV5 <- Sys.getenv("Pardot_TokenV5")
pardotBusinessID <- Sys.getenv("Pardot_Business_ID")
header4 <- c("Authorization" = pardotTokenV4, "Pardot-Business-Unit-Id" = pardotBusinessID)
header5 <- c("Authorization" = pardotTokenV5, "Pardot-Business-Unit-Id" = pardotBusinessID)

#' Google Authentication
options(gargle_oauth_cache = ".secrets")
gs4_auth(cache = ".secrets", email = "sazhu24@amherst.edu")

#' GA Authentication
ga_auth(email = "sara.zhu@rmi.org")

#' SF Authentication
sf_auth()

###

#' set Google sheet
ss <- 'https://docs.google.com/spreadsheets/d/1FtZQKYp4ESsY5yQzKuvGT5TorKSyMdvRo4bxg6TI7DU/edit?usp=sharing'

#' set mode
#' - standard mode binds data to existing rows
#' - development mode overwrites all data in sheet
mode <- 'development'

#### SET CAMPAIGN ####

#' current options:
#' 1. OCI 
#' 2. Coal v Gas

campaigns <- c('OCI', 'Coal v Gas')
campaign <- campaigns[2]

#### READ CAMPAIGN KEY ####
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


#### GOOGLE ANALYTICS ####
message('GETTING GOOGLE ANALYTICS DATA')

#' SUMMARY
#' 
#' 1. Pulls list of pages from campaign key file
#' 2. Gets page title and metadata by webscraping provided URLs
#' 3. For all pages
#'      Get key metrics - page views, users, engagement duration
#'      Get acquisition data - sessions + conversions - broken down by channel
#'      Get social media acquisition data - sessions
#'      Get page views broken down by country and region
#'      Get page traffic - sessions - driven by referral sources that have been identified as “Media” 
#'        - These sources are defined in the referralSites file
#' 4. If a new property ID is provided in the campaign key, process is repeated for the new website
#'    and datasets are combined
#' 5. write dataset

#' set GA variables and property ID
rmiPropertyID <- 354053620
metadataGA4 <- ga_meta(version = "data", rmiPropertyID)
dateRangeGA <- c("2023-01-01", paste(currentDate))

#' get referral sites
referralSites <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/referralSites.xlsx') 

#' define site using property ID
campaignPages <- campaignPages %>% 
  mutate(site = ifelse(propertyID == rmiPropertyID, 'rmi.org', sub('/(.*)', '', sub('(.*)https://', '', page)))) %>% 
  filter(!is.na(propertyID))

pageData <- data.frame(site = campaignPages$site, 
                       pageURL = campaignPages$page, 
                       pageTitle = '', 
                       pageType = '', 
                       metadata = '')

pageData <- getPageData(pageData)

#' set page titles
rmiPages <- pageData %>% filter(site == 'rmi.org')
pages <- rmiPages[['pageTitle']]

#' get page metrics
pageMetrics <- getPageMetrics(rmiPropertyID, pages) 

#' get acquisition
acquisition <- getAcquisition(rmiPropertyID, pages) 

#' get social traffic
socialTraffic <- getTrafficSocial(rmiPropertyID, pages) 

#' get geographic segments
geographyTraffic <- getTrafficGeography(rmiPropertyID, pages) 

#' get referrals
mediaReferrals <- getReferrals(rmiPropertyID, pages)

#' get data for new website if a new GA property ID is provided
if(length(propertyIDs) > 1){
  
  #' set property ID for new website
  sitePropertyID <- propertyIDs[2]
  
  #' get pages
  newSitePages <- pageData %>% filter(site != 'rmi.org')
  pages <- unique(newSitePages[['pageTitle']])
  
  #' get website URL
  newSiteURL <- unique(newSitePages[['site']])
  
  ###
  
  #' get page metrics + acquisition
  pageMetricsNS <- getPageMetrics(sitePropertyID, pages) %>% 
    distinct()
  
  pageMetrics <- pageMetrics %>% rbind(pageMetricsNS)
  
  #' get acquisition
  acquisitionNS <- getAcquisition(sitePropertyID, pages, site = newSiteURL) 
  acquisition <- acquisition %>% rbind(acquisitionNS)
  
  #' bind page metrics and pivot table so that sessions/conversions are stored in one column
  #' this is to make a Power BI table column that changes based on an applied filter
  allTraffic <- pageData %>% 
    select(site, pageTitle) %>% 
    distinct() %>% 
    plyr::rbind.fill(acquisition) %>% 
    pivot_longer(cols = c(Sessions:'Form Submissions'), names_to = "type", values_to = "count") %>% 
    mutate(count = round(count, 1)) %>% 
    left_join(select(pageMetrics, c(pageTitle, screenPageViews:avgEngagementDuration, pageType, icon)), by = 'pageTitle') %>% 
    mutate(count = as.numeric(ifelse(is.na(count), 0, count))) %>% 
    filter(defaultChannelGroup != 'Unassigned' & !is.na(defaultChannelGroup))
  
  numChannels <- allTraffic %>% group_by(pageTitle, type) %>% summarize(numChannels = n())
  
  allTraffic <- allTraffic %>% 
    left_join(numChannels) %>% 
    mutate('Page Views' = round(screenPageViews/numChannels, 2)) %>% 
    select(-c(screenPageViews, numChannels))
  
  #' get social media acquisition and bind
  socialTrafficNS <- getTrafficSocial(sitePropertyID, pages, site = newSiteURL) 
  
  socialTraffic <- socialTraffic %>% 
    rbind(socialTrafficNS)
  
  #' get page traffic geography and bind
  geographyTrafficNS <- getTrafficGeography(sitePropertyID, pages, site = newSiteURL) 
  
  geographyTraffic <- geographyTraffic %>% 
    rbind(geographyTrafficNS)
  
  #' get media referrals and bind
  mediaReferralsNS <- getReferrals(sitePropertyID, pages, site = newSiteURL)
  
  mediaReferrals <- mediaReferrals %>% 
    rbind(mediaReferralsNS)
  
} else {
  #' bind page metrics and pivot table so that sessions/conversions are stored in one column
  #' this is to make a Power BI table column that changes based on an applied filter
  allTraffic <- pageData %>% 
    select(site, pageTitle) %>% 
    distinct() %>% 
    plyr::rbind.fill(acquisition) %>% 
    pivot_longer(cols = c(Sessions:'Form Submissions'), names_to = "type", values_to = "count") %>% 
    mutate(count = round(count, 1)) %>% 
    left_join(select(pageMetrics, c(pageTitle, screenPageViews:avgEngagementDuration, pageType, icon)), by = 'pageTitle') %>% 
    mutate(count = as.numeric(ifelse(is.na(count), 0, count))) %>% 
    filter(defaultChannelGroup != 'Unassigned' & !is.na(defaultChannelGroup)) 
  
  numChannels <- allTraffic %>% group_by(pageTitle, type) %>% summarize(numChannels = n())
  
  allTraffic <- allTraffic %>% 
    left_join(numChannels) %>% 
    mutate('Page Views' = round(screenPageViews/numChannels, 2)) %>% 
    select(-c(screenPageViews, numChannels))
  
}


#' push data
message('PUSHING GOOGLE ANALYTICS DATA')

ALL_WEB_TRAFFIC <- pushData(allTraffic, 'Web Traffic - All')
ALL_WEB_SOCIAL <- pushData(socialTraffic, 'Web Traffic - Social')
ALL_WEB_GEO <- pushData(geographyTraffic, 'Web Traffic - Geography')
ALL_WEB_REFERRALS <- pushData(mediaReferrals, 'Web Traffic - Referrals')


####'EMAIL NEWSLETTERS ####
message('GETTING NEWSLETTERS DATA')

#' SUMMARY
#' 
#' 1. Get all newsletters - Spark + Market Catalyst - from Newsletter Stats file
#' 2. Filter URLs to get newsletters that contain the page URLs in this campaign
#' 3. Write dataset

#' get all email stats 

if(!exists('allEmailStats')){
  allEmailStats <- getAllEmailStats()
}

#' get newsletter story URLs
pageURLs <- pageData$pageURL

#' get newsletter stories that match page URLS
campaignNewsletters <- getCampaignEmails(pageURLs)

#' Set hasEmail to FALSE if no emails detected
if(nrow(campaignNewsletters) == 0) hasEmail <- FALSE else hasEmail <- TRUE

if(hasEmail == TRUE){
  #' push data
  message('PUSHING NEWSLETTER DATA')
  
  ALL_NEWSLETTERS <- pushData(campaignNewsletters, 'Newsletters')
}



#### SALESFORCE ####
message('GETTING SALESFORCE DATA')

#' SUMMARY
#' 
#' 1a Pull Salesforce campaign IDs for reports provided in campaign key
#'    Get affiliated campaign members and binds data from Salesforce Contacts/Leads/Accounts query
#' 1b Pull Salesforce campaign IDs for events provided in campaign key
#'    Get affiliated campaign members and binds data from Salesforce Contacts/Leads/Accounts query
#' 1c Pull Pardot list email IDs from the campaignNewsletters dataframe generated by the getCampaignNewsletters function
#'    Get all link clicks for newsletters from Pardot
#'    Get all prospects - contacts and leads with pardot activity - using getProspects function
#'    Joins dataframes using Pardot ID from the Pardot URL field to get contact/lead information
#' 2. Cleans data and joins account data for unknown contacts/leads using email domains 
#'    Categorizes audience groups using the govDomains file and audienceGroups file
#' 3. Bind all dataframes together
#' 4. Get donations - view process summary in Donations section
#' 5. Write dataset

if(hasReport == TRUE | hasEvent == TRUE | hasEmail == TRUE){
  
  #' create dataframe for campaigns
  df <- as.data.frame(matrix(0, ncol = 19, nrow = 0))
  names(df) <- c('CampaignName', 'EngagementType', 'Id', 'RecordType', 'Status', 'EngagementDate', 
                 'Name', 'Email', 'Domain', 'Account', 'AccountType', 'Industry', 'TotalGiving', 
                 'NumOpenOpps', 'Pardot_Score', 'Pardot_URL', 'Giving_Circle', 'Last_Gift', 'AccountId') 
  
  #' get list of campaigns
  campaignList <- getCampaignList()
  
  #' get all accounts
  if(!exists('all_accounts')){
    all_accounts <- getAllAccounts() 
  }
  
  #' remove accounts with duplicate names to avoid errors when joining by Account name
  accountsUnique <- all_accounts[!duplicated(all_accounts$Account) & !duplicated(all_accounts$Account, fromLast = TRUE),] %>% 
    filter(!grepl('unknown|not provided|contacts created by revenue grid', Account))
  
  #' get domain info for gov accounts
  govDomains <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/govDomains.xlsx') 
  
  #' get audience domains and accounts
  audienceGroups <- read.xlsx('/Users/sara/Desktop/GitHub/RMI_Analytics/files/audienceGroups.xlsx')
  audienceAccounts <- audienceGroups %>% select(Account, type) %>% filter(!is.na(Account)) %>% distinct(Account, .keep_all = TRUE)
  audienceDomains <- audienceGroups %>% select(Domain, type) %>% filter(!is.na(Domain)) %>% distinct(Domain, .keep_all = TRUE)

  #' get campaign member data for reports, events, and newsletter clicks
  if(hasReport == TRUE){
    campaignMembersReports <- getSalesforceReports()
    df <- df %>% rbind(campaignMembersReports)
  } 
  
  if(hasEvent == TRUE) {
    campaignMembersEvents <- getSalesforceEvents()
    df <- df %>% rbind(campaignMembersEvents)
  } 
  
  if(hasEmail == TRUE) {
    #' get all prospects
    if(!exists('prospects')){
      prospects <- getProspects() %>% 
        mutate(Domain = sub("(.*)\\@", "", Email))
    }
    
    campaignMembersNewsletters <- getCampaignNewsletters()
    df <- df %>% rbind(campaignMembersNewsletters)
  }
  
  #' remove duplicates
  SFcampaigns <- df %>% distinct(Id, CampaignName, .keep_all = TRUE)

  #' get donation revenue attributed to these campaign components
  donations <- getAttributedDonationValue(SFcampaigns)
  
  #' bind donations to SF campaign data
  donationsByCampaignMember <- donations %>% 
    group_by(Id, CampaignName) %>% 
    summarize(AttributtedDonationValue = sum(AttributtedValue))
  
  SFcampaigns <- SFcampaigns %>% 
    left_join(donationsByCampaignMember) %>% 
    select(CampaignName, EngagementType, Icon, Id, Status, EngagementDate, Domain, Email, 
           DonorType, AttributtedDonationValue, AccountId, Account, AccountType, Audience1, Audience2, Industry, 
           Pardot_URL, Pardot_ID, GivingCircleTF, SolutionsCouncilTF, InnovatorsCircleTF, OpenOppTF, DonorTF, 
           LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) 
}



#' push data
message('PUSHING SALESFORCE DATA')

ALL_SALESFORCE <- pushData(SFcampaigns, 'Salesforce')
ALL_DONATIONS <- pushData(donations, 'SF Donations')



#### SOCIAL MEDIA ####
message('GETTING SOCIAL MEDIA DATA')

#' SUMMARY
#' 
#' 1. Get all posts made after Jan 1, 2023 with tags from all social channels except for program LinkedIn accounts
#' 2. Request does not return the post account/profile ID so use post links - perma_link - to identify the account
#     NOTE: This works for all platforms except LinkedIn, which doesn't include account info in post URLs
#' 3. Get all posts for program Linkedin accounts, add account identifier, then bind all of these to posts retrieved in step 1
#' 4. Get averages for all get posts made over the last year, left join these values to all posts df
#' 6. Filter by campaign tag to get campaign posts
#' 7. Write dataset

#' get all sprout social tags
metadeta <- getMetadata(url = 'metadata/customer/tags')
tags <- metadeta[["data"]]

#' get post averages based on all posts made over the last year 
posts1YRaverages <- getPostAverages()

#' get all social media posts with tags except for posts made from program LinkedIn accounts
allPostsTags <- getAllSocialPosts()

#' get all posts from linkedIn program channels
linkedInTagged <- getLIProgramPosts('tagged')

#' clean and bind linkedin program posts to all posts
taggedPosts <- cleanPostDF(allPostsTags, 'tagged') %>% 
  rbind(linkedInTagged) %>% 
  left_join(posts1YRaverages, by = c('post_type', 'account')) %>% 
  #' calculate post performance compared to avgs.
  mutate(impressionsVmedian = round((impressions - impressionsMedian)/impressionsMedian, 3),
         engagementsVmedian = round((engagements - engagementsMedian)/engagementsMedian, 3),
         engrtVmedian = round((engagementRate - engrtMedian)/engrtMedian, 3),
         impressionsVmean = round((impressions - impressionsMean)/impressionsMean, 3),
         engagementsVmean = round((engagements - engagementsMean)/engagementsMean, 3),
         engrtVmean = round((engagementRate - engrtMean)/engrtMean, 3),
         brand = ifelse(grepl('RMI Brand', account), 1, 0),
         program = ifelse(brand == 1, 0, 1),
         accountType = ifelse(brand == 1, 'RMI Brand', 'RMI Program'),
         post = 'Post') %>% 
  select(created_time, account, post_type, icon, tag_id, tag_name, 
         impressions, impressionsVmedian, impressionsVmean, engagements, engagementsVmedian, engagementsVmean, 
         engagementRate, engrtVmedian, engrtVmean, postClicks, shares, 
         brand, program, accountType, post, perma_link, text)

#' find tagged posts for this campaign
if(campaign == 'OCI'){
  #' OCI posts were not tagged properly so get these by creating a custom filter
  campaignPosts <- taggedPosts %>% 
    filter(created_time >= '2023-04-05' & grepl('OCI\\+', text)) %>% 
    distinct(perma_link, .keep_all = TRUE) 
} else{
  campaignPosts <- taggedPosts %>% 
    filter(tag_name == socialTag) %>% 
    distinct(perma_link, .keep_all = TRUE) 
}

#' push data
message('PUSHING SOCIAL MEDIA DATA')

ALL_SOCIAL_POSTS <- pushData(campaignPosts, 'Social Media Posts')


####' Monday.com ####
message('GETTING MONDAY.COM DATA')

#' SUMMARY
#' 
#' 1. Get all projects from Active Project Board
#' 2. Iterate through this board to find projects with "Metrics Dashboard" in the promotion tactics column
#' 3. Retrieve ID, audiences, and promotion tactics from these projects
#' 4. Filter for project that contains campaign ID
#' 5. Write data set

#' get Active Projects Board
query <- "query { boards (ids: 2208962537) { items { id name column_values{ id value text } } } } "
res <- getMondayCall(query)
activeProjects <- as.data.frame(res[["data"]][["boards"]][["items"]][[1]])

#' loop through Active Projects Board to find projects with "Metrics Dashboard" in the promotion tactics column
projects <- data.frame(id = '', row = '', name = '')[0,]

for(i in 1:nrow(activeProjects)){
  
  board <- activeProjects[[3]][[i]]
  if(grepl('Metrics Dashboard', paste(board[11, 'text']))){
    projects <- projects %>% 
      rbind(c(paste(activeProjects[i, 'id']), i, c(paste(activeProjects[i, 'name'])))) 
  }
}

names(projects) <- c('id', 'row', 'name')

for(i in 1:nrow(projects)){
  
  metricsDashboardCampaigns <- data.frame(CAMPAIGN_ID = '', ID = '', audiences = '', promotionTactics = '')[0,]
  
  #' get promotion tactics, audiences, and ID
  campaignRow <- as.numeric(targetCampaign[1, 'row'])
  campaignBoard <- activeProjects[[3]][[campaignRow]]
  campaignDF <- data.frame(CAMPAIGN_ID = campaignID,
                           ID = campaignBoard[16, 'text'], 
                           audiences = campaignBoard[15, 'text'], 
                           promotionTactics = campaignBoard[11, 'text']) 
  
  metricsDashboardCampaigns <- metricsDashboardCampaigns %>% rbind(campaignDF)
  
}

metricsDashboardCampaigns <- metricsDashboardCampaigns %>% 
  mutate(promotionTactics = gsub(', Metrics Dashboard', '', promotionTactics))

#' filter by campaign ID
targetCampaign <- metricsDashboardCampaigns %>% 
  filter(CAMPAIGN_ID == campaignID)

#' push data
message('PUSHING MONDAY.COM DATA')

ALL_MONDAY <- pushData(targetCampaign, 'Campaign Overview')


#### CREATE CONTENT SUMMARY ####

#' SUMMARY
#' 
#' 1. Create content summary table for Campaign Summary tab on dashboard
#'    by getting instances of all social media posts, Salesforce campaigns - all reports, events, or newsletters - 
#'    and media referrals 
#' 2. Write dataset

socialContent <- campaignPosts %>% 
  mutate(type = 'Social Media Posts') %>% 
  select(type, name = post_type) 

salesforceContent <- SFcampaigns %>% 
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

message('PUSHING CONTENT SUMMARY')
ALL_CONTENT_SUMMARY <- pushData(contentSummary, 'Content Summary')


