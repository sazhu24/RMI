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
mondayToken <- ###
  
# Sprout Social Token
sproutToken <- ###
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")
currentDate <- paste(Sys.Date())
yearAgo <- ymd(currentDate) - years(1)

# Pardot API Token & Request Headers
token4 <- ###
token5 <- ###
businessID <- ###
header4 <- c("Authorization" = token4, "Pardot-Business-Unit-Id" = businessID)
header5 <- c("Authorization" = token5, "Pardot-Business-Unit-Id" = businessID)

## Google Authentication
options(gargle_oauth_cache = ".secrets")
gs4_auth(cache = ".secrets", email = "sazhu24@amherst.edu")

## GA Authentication
ga_auth(email = "sara.zhu@rmi.org")

### 

campaign <- 'OCI'

## push data to this Google sheet
ss <- 'https://docs.google.com/spreadsheets/d/1BzLkm4jZr1WwMQsC4hBweKkvnBsWq4ZXlbyg7BYMsoA/edit?usp=sharing' ## OCI

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


##### WEB
print('GET GOOGLE ANALYTICS DATA')

### set GA variables and property ID
rmiPropertyID <- 354053620
metadataGA4 <- ga_meta(version = "data", rmiPropertyID)
currentDate <- Sys.Date()
dates1 <- c("2023-01-01", paste(currentDate))

### get referral sites
referralSites <- read_sheet('https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843', sheet = 'All Referral Sites')

### FUNCTIONS

## get web traffic and key metrics for all pages
getPageMetrics <- function(propertyID, pages){
  campaignPages <- ga_data(
    propertyID,
    metrics = c('screenPageViews', "totalUsers", "userEngagementDuration", 'sessions'),
    dimensions = c("pageTitle"),
    date_range = dates1,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    mutate(engagementDuration = userEngagementDuration / totalUsers,
           sec = round(engagementDuration %% 60, 0),
           min = (engagementDuration / 60) |> floor(),
           avgEngagementDuration = paste0(min, ':', sec)) %>% 
    select(pageTitle, screenPageViews, totalUsers, engagementDuration, avgEngagementDuration) %>% 
    left_join(select(pageData, c(pageTitle, pageType, icon)), by = c('pageTitle')) %>% 
    mutate(pageTitle = gsub(' - RMI', '', pageTitle))
  
  return(campaignPages)
}

## function - correct social media and email channel traffic
correctTraffic <- function(df, type){
  if(type == 'session'){
    df <- df %>% 
      dplyr::rename(medium = sessionMedium) %>% 
      dplyr::rename(source = sessionSource) %>% 
      dplyr::rename(defaultChannelGroup = sessionDefaultChannelGroup)
  } 
  
  df <- df %>% 
    mutate(pageTitle = gsub(' - RMI', '', pageTitle)) %>% 
    mutate(medium = ifelse(grepl('mail.google.com', source)|grepl('web-email|sf|outlook', medium), 'email', medium),
           source = ifelse(grepl('linkedin|lnkd.in', source), 'linkedin', source),
           source = ifelse(grepl('facebook', source), 'facebook', source),
           source = ifelse(grepl('dlvr.it|twitter', source)|source == 't.co', 'twitter', source),
           medium = ifelse(grepl('linkedin|lnkd.in|facebook|twitter|instagram', source)|grepl('twitter|fbdvby', medium), 'social', medium),
           medium = ifelse(grepl('/t.co/', pageReferrer), 'social', medium),
           source = ifelse(grepl('/t.co/', pageReferrer), 'twitter', source),
           source = ifelse(grepl('instagram', source), 'instagram', source),
           defaultChannelGroup = ifelse(medium == 'social', 'Organic Social', 
                                        ifelse(medium == 'email', 'Email', defaultChannelGroup))) 
  
  return(df)
}

getTrafficSocial <- function(propertyID, pages, site = 'rmi.org'){
 
   ## get social traffic
  aquisitionSocial <- ga_data(
    propertyID,
    metrics = c("sessions", "screenPageViews"),
    dimensions = c("pageTitle", "sessionSource", "sessionMedium", 'pageReferrer', 'sessionDefaultChannelGroup'),
    date_range = dates1,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    arrange(pageTitle) 
  
  aquisitionSocial <- correctTraffic(aquisitionSocial, type = 'session') %>% 
    filter(medium == 'social') %>% 
    dplyr::group_by(pageTitle, source) %>% 
    dplyr::summarize(Sessions = sum(sessions),
                     PageViews = sum(screenPageViews)) %>% 
    mutate(site = site,
           dashboardCampaign = campaignID)
  
  return(aquisitionSocial)
}

getTrafficGeography <- function(propertyID, pages, site = 'rmi.org'){
  
  ## get page views by region
  trafficByRegion <- ga_data(
    propertyID,
    metrics = c('screenPageViews'),
    dimensions = c("pageTitle", 'region', 'country'),
    date_range = dates1,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    filter(screenPageViews > 4) %>% 
    arrange(pageTitle) %>% 
    dplyr::rename('Region Page Views' = screenPageViews) %>% 
    mutate(pageTitle = gsub(' - RMI', '', pageTitle),
           site = site, 
           dashboardCampaign = campaignID)
  
  return(trafficByRegion)
}

getAcquisition <- function(propertyID, pages, site = 'rmi.org'){
  
  ## get acquisition sessions
  aquisitionSessions <- ga_data(
    propertyID,
    metrics = c("sessions"),
    dimensions = c("pageTitle", "sessionSource", "sessionMedium", "pageReferrer", 'sessionDefaultChannelGroup'),
    date_range = dates1,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    arrange(pageTitle) 
  
  acquisition <- correctTraffic(aquisitionSessions, 'session') %>% 
    group_by(pageTitle, defaultChannelGroup) %>% 
    summarize(Sessions = sum(sessions))
  
  if(site == 'rmi.org'){
    
    # 2) get conversions
    aquisitionConversions <- ga_data(
      propertyID,
      metrics = c('conversions:form_submit', 'conversions:file_download'),
      dimensions = c("pageTitle", "source", "medium", 'pageReferrer', 'defaultChannelGroup'),
      date_range = dates1,
      dim_filters = ga_data_filter("pageTitle" == pages),
      limit = -1
    ) %>% 
      select(pageTitle, source, medium, pageReferrer, defaultChannelGroup, 
             form_submit = 'conversions:form_submit', download = 'conversions:file_download') %>% 
      arrange(pageTitle)
    
    aquisitionConversions <- correctTraffic(aquisitionConversions, 'conversion') %>% 
      dplyr::group_by(pageTitle, defaultChannelGroup) %>% 
      dplyr::summarize('Downloads' = sum(download),
                       'Form Submissions' = sum(form_submit)) 
    
    # 3) bind rows: sessions + conversions 
    acquisition <- acquisition %>% 
      left_join(aquisitionConversions, by = c('pageTitle', 'defaultChannelGroup')) 
    
  }
  
  acquisition <- acquisition %>% 
    mutate(site = site)
  
  return(acquisition)
  
}

getReferrals <- function(propertyID, pages, site = 'rmi.org'){
  
  referrals <- ga_data(
    propertyID,
    metrics = c("sessions"),
    dimensions = c("pageTitle", "sessionSource", "sessionMedium", "pageReferrer"),
    date_range = dates1,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    arrange(pageTitle) %>% 
    group_by(pageTitle, sessionSource) %>% 
    filter(sessionMedium == 'referral') %>% 
    inner_join(select(referralSites, c(media, sessionSource, mediaType, mediaSubtype)), by = 'sessionSource') %>% 
    mutate(referrer = sub('(.*)https://', '', pageReferrer),
           referrer = sub('/(.*)', '', referrer)) %>% 
    filter(referrer != 'rmi.org') %>% 
    group_by(pageTitle, sessionSource, media, mediaType, mediaSubtype) %>% 
    summarise(sessions = sum(sessions)) %>% 
    filter(sessions > 2) %>% 
    mutate(site = site)
  
  return(referrals)
  
}


###

campaignPages <- campaignPages %>% 
  mutate(site = ifelse(propertyID == rmiPropertyID, 'rmi.org', sub('/(.*)', '', sub('(.*)https://', '', page)))) %>% 
  filter(!is.na(propertyID))

pageData <- data.frame(site = campaignPages$site, 
                       pageURL = campaignPages$page, 
                       pageTitle = '', 
                       pageType = '', 
                       metadata = '')

for(i in 1:nrow(pageData)){
  
  url <- as.character(pageData[i, 'pageURL'])
  
  ## get page titles
  tryCatch( { 
    url_tb <- url %>%
      read_html() %>% 
      html_nodes('head > title') %>% 
      html_text() %>% 
      as.data.frame() %>% 
      rename(title = 1) 
    
    pageData[i, 'pageTitle'] <- url_tb[1, 'title']
    
  }, error = function(e){
    pageData[i, 'pageTitle'] <- NA
  })
  
  ## get page types from metadata
  if(pageData[i, 'site'] == 'rmi.org'){
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
      
      pageData[i, 'metadata'] <- url_tb[2, 'keywords']
      
    }, error = function(e){
      pageData[i, 'metadata'] <- NA
    })
  } else {
    pageData[i, 'metadata'] <- NA
    pageData[i, 'pageType'] <- 'New Website'
  }
  
}

pageData <- pageData %>% 
  mutate(pageType = ifelse(grepl('article', tolower(metadata)), 'Article',
                           ifelse(grepl('report', tolower(metadata)), 'Report', pageType)),
         icon = ifelse(grepl('article', tolower(metadata)), 4,
                       ifelse(grepl('report', tolower(metadata)), 1, 5))) %>% 
  distinct(pageTitle, .keep_all = TRUE)

## set page titles
rmiPages <- pageData %>% filter(site == 'rmi.org')
pages <- rmiPages[['pageTitle']]

#
pageMetrics <- getPageMetrics(rmiPropertyID, pages) 
totalPageViews <- sum(pageMetrics$screenPageViews)

#
acquisition <- getAcquisition(rmiPropertyID, pages) 

#
socialTraffic <- getTrafficSocial(rmiPropertyID, pages) 

#
geographyTraffic <- getTrafficGeography(rmiPropertyID, pages) 

#
mediaReferrals <- getReferrals(rmiPropertyID, pages)


###

if(length(propertyIDs > 1)){
  
  # set property ID for new website
  property_id <- propertyIDs[2]
  sitePropertyID <- property_id
  
  # get pages
  otherPages <- pageData %>% filter(site != 'rmi.org')
  pages <- unique(otherPages[['pageTitle']])
  
  # get website URL
  newSiteURL <- unique(otherPages[['site']])
  
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

getAllEmailStats <- function(){
  
  # find link on Email Stats spreadsheet
  emailStatsSpark <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Spark Stats (Unformatted)') %>% 
    mutate(date = as.Date(date))
  
  # find link on Email Stats spreadsheet
  emailStatsPEM <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Market Catalyst Stats (Unformatted)') %>% 
    mutate(name = ifelse(grepl('PEM 2023-07 Finance Newsletter', name), 'PEM 2023-07-09 Finance Newsletter', name),
           date = as.Date(stringr::str_extract(name, '[0-9]+-[0-9]+-[0-9]+')))
  
  allEmailStats <- emailStatsSpark %>% 
    plyr::rbind.fill(emailStatsPEM) %>% 
    mutate(name = ifelse(grepl('Spark', name), paste0(date, ': Spark'), paste0(date, ':', sub('(.*)-[0-9]{2}', '', name))),
           id = as.numeric(id))
  
  return(allEmailStats)
}

## get all email stats 
allEmailStats <- getAllEmailStats()

## get newsletter story URLs
pageURLs <- pageData$pageURL

## filter to grab emails and story URLs
df1 <- allEmailStats %>%
  filter(grepl(paste(pageURLs, collapse = '|'), url_1)) %>% 
  select(c(1:22))

colnames(df1)[c(19:22)] <- c("story_url", "story_title", "story_clicks", "story_COR")

df2 <- allEmailStats %>%
  filter(grepl(paste(pageURLs, collapse = '|'), url_2)) %>% 
  select(c(1:18, 23:26))

colnames(df2)[c(19:22)] <- c("story_url", "story_title", "story_clicks", "story_COR")

df3 <- allEmailStats %>%
  filter(grepl(paste(pageURLs, collapse = '|'), url_3)) %>% 
  select(c(1:18, 27:30))

colnames(df3)[c(19:22)] <- c("story_url", "story_title", "story_clicks", "story_COR")

# bind 
allStoryStats <- df1 %>% 
  rbind(df2) %>% 
  rbind(df3) %>% 
  mutate(date = as.Date(date),
         icon = '',
         story_title = gsub(' - RMI', '', story_title))

# add icon 
allStoryStats <- allStoryStats[rev(order(allStoryStats$date)),]
rownames(allStoryStats) <- NULL
allStoryStats[, 'icon'] <- as.numeric(rownames(allStoryStats))

# set hasEmail to FALSE if no emails detected
if(nrow(allStoryStats) == 0) hasEmail <- FALSE else hasEmail <- TRUE

# bind campaign ID
allStoryStats <- allStoryStats %>% 
  mutate(dashboardCampaign = campaignID)

# push data
print('push email stats data')

write_sheet(allStoryStats, ss = ss, sheet = 'Campaign Email Stats')


### SALESFORCE
print('GET SALESFORCE DATA')

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
  
  my_soql <- sprintf("SELECT Id, Name, Account_Name_Text__c, Email,
                             pi__url__c, pi__score__c, pi__last_activity__c,
                             Giving_Circle__c, Number_of_Days_since_Last_Gift_or_SC__c, AccountId
                           
                    FROM Contact
                    WHERE pi__last_activity__c != null 
                    ORDER BY pi__last_activity__c DESC")
  
  allContacts <- sf_query(my_soql, 'Contact', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Account_Name_Text__c, Email, 
           Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, 
           Giving_Circle = Giving_Circle__c, Last_Gift = Number_of_Days_since_Last_Gift_or_SC__c,
           AccountId) %>% 
    mutate(RecordType = 'Contact')
  
  my_soql <- sprintf("SELECT Id,  Name, Company, Email, pi__url__c, pi__score__c, pi__last_activity__c

                    FROM Lead
                    WHERE pi__last_activity__c != null 
                    ORDER BY pi__last_activity__c DESC")
  
  allLeads <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c) %>% 
    mutate(RecordType = 'Lead') %>% 
    anti_join(allContacts, by = 'Pardot_URL')
  
  prospects <- allContacts %>% 
    plyr::rbind.fill(allLeads) %>% 
    filter(!is.na(Email))
  
  # identify and remove duplicates
  dup <- prospects[duplicated(prospects[,c("Pardot_URL")]) | duplicated(prospects[,c("Pardot_URL")], fromLast = TRUE), ] 
  
  dup1 <- dup %>% 
    filter(grepl('unknown|not provided|contacts created', tolower(Account))|Account == ''|is.na(Account)) %>% 
    distinct(Pardot_URL, .keep_all = TRUE)
  
  prospects_keep <- prospects %>% 
    anti_join(dup1) 
  
  unique_prospects <- prospects_keep %>% 
    distinct(Pardot_URL, .keep_all = TRUE)
  
  ## 188,615 unique prospects
  
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
  
  all_accounts <- sf_query(my_soql, "Account", api_type = "Bulk 1.0") %>% 
    select(AccountId = Id, Account = Name, AccountType = Type, Industry, AccountDomain = Email_Domain__c,
           Website, DB_Website = D_B_Web_Address__c, DB_IndustryCategory = D_B_Major_Industry_Category_Name__c, NAICS1 = D_B_NAICS_Description_1__c, 
           NAICS2 = D_B_NAICS_Description_2__c, SIC1 = D_B_SIC4_Code_1_Description__c, SIC2 = D_B_SIC4_Code_2_Description__c,
           TopAccount = Top_Account__c, TotalGiving = Total_Opportunity_Payments__c, NumGifts = of_Gifts__c, NumOpenOpps = Number_of_Open_Opportunities__c)
  
  return(all_accounts)
}

### SF CAMPAIGNS (REPORTS AND EVENTS)

# get list of campaigns
getCampaignList <- function(){
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
  
  campaignList <- sf_query(my_soql) %>% 
    select(Id, Name, Type, CreatedDate, Members = Members_in_Campaign__c)
  
  return(campaignList)
}

# get campaign members and join to contact/lead/account info
getCampaignMembers <- function(campaignIdList, campaignType) {
  #campaignIdList <- campaignReports$reportID
  # get campaign members
  my_soql <- sprintf("SELECT CampaignId,
                             Name,
                             Status,
                             HasResponded,
                             ContactId,
                             LeadId,
                             CreatedDate

                      FROM CampaignMember
                      WHERE CampaignId in ('%s')",
                     paste0(campaignIdList, collapse = "','"))
  
  campaign_members <- sf_query(my_soql, 'CampaignMember', api_type = "Bulk 1.0") %>% 
    left_join(select(campaignList, c(CampaignId = Id, CampaignName = Name)), by = 'CampaignId') %>% 
    filter(!(is.na(as.character(ContactId)) & is.na(LeadId)))
  
  campaignContacts <- campaign_members %>% 
    filter(!is.na(ContactId)) %>% 
    distinct(ContactId, .keep_all = TRUE) 
  
  campaignLeads <- campaign_members %>% 
    filter(!is.na(LeadId) & is.na(ContactId)) %>% 
    distinct(LeadId, .keep_all = TRUE) 
  
  my_soql <- sprintf("SELECT Id, Name, Account_Name_Text__c, Email,
                             pi__url__c, pi__score__c, pi__last_activity__c,
                             Giving_Circle__c, Number_of_Days_since_Last_Gift_or_SC__c, AccountId
                           
                    FROM Contact
                    WHERE Id in ('%s')
                    ORDER BY pi__last_activity__c DESC NULLS LAST",
                    paste0(campaignContacts$ContactId, collapse = "','"))
  
  campaignContactsQuery <- sf_query(my_soql, 'Contact', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Account_Name_Text__c, Email, 
           Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, 
           Giving_Circle = Giving_Circle__c, Last_Gift = Number_of_Days_since_Last_Gift_or_SC__c,
           AccountId) %>% 
    mutate(RecordType = 'Contact') %>% 
    filter(!is.na(AccountId))
  
  my_soql <- sprintf("SELECT Id,  Name, Company, Email, pi__url__c, pi__score__c, pi__last_activity__c

                      FROM Lead
                      WHERE Id in ('%s')
                      ORDER BY pi__last_activity__c DESC NULLS LAST",
                      paste0(campaignLeads$LeadId, collapse = "','"))
  
  campaignLeadsQuery <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c) %>% 
    mutate(RecordType = 'Lead') 
  
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
           TotalGiving = Total_Opportunity_Payments__c, NumOpenOpps = Number_of_Open_Opportunities__c)
  
  campaignContactsQuery <- campaignContactsQuery %>% 
    left_join(select(campaignContactAccounts, -Account), by = c('AccountId'))
  
  campaignLeadsQuery <- campaignLeadsQuery %>% 
    left_join(campaignContactAccounts, by = c('Account')) 

  contactsLeads <- campaignContactsQuery %>%
    plyr::rbind.fill(campaignLeadsQuery) 
  
  CampaignMembers <- campaign_members %>% 
    mutate(Id = ifelse(is.na(ContactId), LeadId, ContactId)) %>% 
    select(CampaignId, CampaignName, Name, Id, Status, CreatedDate) %>% 
    left_join(contactsLeads, by = c('Id', 'Name'))
  
  if(campaignType == 'Event'){
    
    CampaignMembers <- CampaignMembers %>% 
      mutate(EngagementType = 'Event',
             Status = ifelse(grepl('Register', Status), 'Registered (Did Not Attend)', Status)) %>% 
      filter(grepl('Register|Attended', Status))
    
  } else if (campaignType == 'Report'){
    
    CampaignMembers <- CampaignMembers %>% 
      mutate(EngagementType = 'Report Download') 
      
  }
  
  CampaignMembers <- CampaignMembers %>% 
    mutate(Domain = sub("(.*)\\@", "", Email),
           EngagementDate = as.Date(CreatedDate, format = "%Y-%m-%d")) %>% 
    select(CampaignName, EngagementType, Id, RecordType, Status, EngagementDate, Name, Email, Domain, Account, AccountType, Industry, 
           TotalGiving, NumOpenOpps, Pardot_Score, Pardot_URL, Giving_Circle, Last_Gift, AccountId)
  
  return(CampaignMembers)
  
}

### get campaigns and clean 
cleanCampaignDF <- function(df){
  
  df <- df %>% 
    filter(!is.na(Domain)) %>% 
    distinct(Id, CampaignName, .keep_all = TRUE) %>% 
    mutate(DonorType = 
             case_when(
               !is.na(Giving_Circle) ~ Giving_Circle,
               !is.na(Last_Gift) ~ 'Donor',
               .default = NA
             ),
           Icon = 
             case_when(
               EngagementType == 'Report Download' ~ 1,
               EngagementType == 'Event' ~ 2, 
               EngagementType == 'Event' ~ 3, 
               .default = NA
             ),
           Pardot_ID = sub("(.*)=", "", Pardot_URL)) %>% 
    left_join(select(all_accounts, c(AccountsName = Account, Domain = AccountDomain, AccountsIndustry = Industry, AccountType2 = AccountType)), by = c('Domain')) %>% 
    mutate(Account = ifelse(grepl('unknown|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account),
           Industry = ifelse(Account == 'Unknown' & !is.na(Industry), AccountsIndustry, Industry),
           AccountType = ifelse(Account == 'Unknown' | (is.na(AccountType) & !is.na(AccountType2)), AccountType2, AccountType)) %>% 
    mutate(Account = ifelse(Account == 'Unknown' & !is.na(AccountsName), AccountsName, Account),
           Account = ifelse(is.na(Account), 'Unknown', Account)) %>% 
    left_join(select(domainKey, c(govDomain = domain, level, govName = name)), by = c('Domain' = 'govDomain')) %>%
    
    mutate(
      Account = 
        case_when(
          is.na(Account) ~ 'Unknown',
          grepl('Household', Account) | Account == 'RMI' ~ 'Household',
          Account == 'Unknown' & !is.na(govName) ~ govName,
          .default = Account
        ),
      Audience1 = 
        case_when(
          level == 'FEDERAL' ~ 'National Gov.',
          level == 'STATE'|grepl('state of|commonwealth of', tolower(Account)) ~ 'State Gov.',
          level == 'LOCAL'|level == 'COUNTY'|grepl('city of|county of', tolower(Account)) ~ 'Local Gov.',
          level == 'INTERNATIONAL' ~ 'International Gov.',
          .default = NA
        )) %>% 
    left_join(select(audienceAccounts, c(Account, type)), by = c('Account')) %>%
    mutate(Audience1 = ifelse(!is.na(type) & is.na(Audience1), type, Audience1)) %>% 
    select(-type) %>% 
    left_join(select(audienceDomains, c(Domain, type)), by = c('Domain')) %>%
    mutate(Audience1 = ifelse(!is.na(type) & is.na(Audience1), type, Audience1)) %>% 
    mutate(
      Audience1 = 
        case_when(
          grepl('Corporate', AccountType) & is.na(Audience1) ~ 'Other Corporate',
          grepl('Foundation', AccountType) & is.na(Audience1)  ~ 'Foundation',
          (grepl('Academic', AccountType) | grepl('edu$', Domain)) & (Account != '' | is.na(Account) | Account != 'Unknown') ~ 'Academic',
          Account == 'Unknown'|is.na(Audience1) ~ 'N/A',
          .default = Audience1
        ),
      Audience2 = ifelse(grepl('Gov', Audience1), 'Government', Audience1)
    ) %>% 
    mutate(DownloadTF = ifelse(grepl('Download', EngagementType), 1, NA),
           EventTF = ifelse(grepl('Attended', Status), 1, NA),
           EmailClickTF = ifelse(grepl('Click', EngagementType), 1, NA),
           GivingCircleTF = ifelse(DonorType == 'Solutions Council'|DonorType == 'Innovators Circle', 1, NA),
           SolutionsCouncilTF = ifelse(DonorType == 'Solutions Council', 1, NA),
           InnovatorsCircleTF = ifelse(DonorType == 'Innovators Circle', 1, NA),
           OpenOppTF = ifelse(NumOpenOpps == ''|NumOpenOpps == 0|is.na(NumOpenOpps), NA, 1),
           DonorTF = ifelse(DonorType == ''|is.na(DonorType), NA, 1),
           LastGift = as.numeric(Last_Gift),
           LapsedDonorsTF = ifelse((LastGift > 549 & LastGift < 1825), 1, NA),
           Engagements = ifelse(!grepl('Registered', Status), 1, NA)) %>% 
    select(CampaignName, EngagementType, Icon, Id, Status, EngagementDate, Domain, Email, 
           DonorType, AccountId, Account, AccountType, Audience1, Audience2, Industry, TotalGiving, Name, Pardot_Score,
           Pardot_URL, Pardot_ID, GivingCircleTF, SolutionsCouncilTF, InnovatorsCircleTF, OpenOppTF, DonorTF, LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) 
  
  return(df)
}

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
  
  df <- df %>% distinct(Id, CampaignName, .keep_all = TRUE) %>% filter(CampaignName != '2023-02-09: Spark')
  
  ## get donations attributed to campaigns
  print('get donations')
  
  donors <- df %>% 
    select(Pardot_ID, Name, DonorType, EngagementType, CampaignName, EngagementDate, Id, Pardot_Score, TotalGiving, Account) %>% 
    distinct() %>% 
    filter(!is.na(DonorType))
  
  getDonations <- function(){
    
    allOpportunities <- data.frame(DonationID = '', DonationDate = '', DonationValue = '', Pardot_ID = '', EngagementDate = '',
                                   EngagementType = '', CampaignName = '', Pardot_Score = '',
                                   TotalGiving = '', DonorType = '')[0,]
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
            cbind(donors[i, 'EngagementDate']) %>% 
            mutate(Pardot_ID = paste(donors[i, 'Pardot_ID']),
                   Id = paste(donors[i, 'Id']),
                   DonationDate = as.Date(DonationDate, format = "%Y-%m-%d"),
                   EngagementDate = as.Date(EngagementDate, format="%Y-%m-%d"),
                   EngagementType = paste(donors[i, 'EngagementType']),
                   CampaignName = paste(donors[i, 'CampaignName']),
                   Pardot_Score = paste(donors[i, 'Pardot_Score']),
                   TotalGiving = paste(donors[i, 'TotalGiving']),
                   DonorType = paste(donors[i, 'DonorType'])) 
          allOpportunities <- allOpportunities %>% rbind(VA)
        }
      }, error = function(e){ NA } )
      
    }
    return(allOpportunities)
  }
  
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

### FUNCTIONS  

## metadata request
getMetadata <- function(url) {
  request <- GET(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                 add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

## make call
getCall <- function(url, args) {
  request <- POST(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                  body = toJSON(args, auto_unbox = TRUE),
                  add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

## post analytics request
sproutPostRequest <- function(page, dateRange, profileIDs, AVG = FALSE){
  
  if(AVG == FALSE) {
    fields <- c(
      "created_time",
      "perma_link",
      "text",
      "internal.tags.id",
      "post_type")
  } else {
    fields <- c(
      "created_time",
      "perma_link",
      "text",
      "post_type")
  }
  
  args <- list(
    "fields" = fields,
    "filters" = c(paste0("customer_profile_id.eq", profileIDs),
                  dateRange),
    "metrics" = c("lifetime.impressions", "lifetime.engagements", "lifetime.post_content_clicks", "lifetime.shares_count"), 
    "timezone" = "America/Denver",
    "page" = paste(page))
  
  getStats <- getCall(url = 'analytics/posts', args = args)
  if(is.null(getStats[["paging"]])) {
    postStats <- NULL
  } else if(AVG == FALSE) {
    metrics <- getStats[["data"]][["metrics"]]
    internal <- getStats[["data"]][["internal"]]
    postStats <- getStats[["data"]] %>% 
      select(-c('metrics', 'internal')) %>% 
      cbind(metrics) %>% 
      cbind(internal)
  } else if(AVG == TRUE) {
    metrics <- getStats[["data"]][["metrics"]]
    postStats <- getStats[["data"]] %>% 
      select(-c('metrics')) %>% 
      cbind(metrics) 
  }
  
  return(postStats)
}

## clean response
cleanDF <- function(df, type, linkedin = 'FALSE'){
  
  posts <- df %>% 
    mutate(engagementRate = round(as.numeric(lifetime.engagements)/as.numeric(lifetime.impressions), 3),
           created_time = as.Date(sub('T(.*)', '', created_time)),
           month = lubridate::month(ymd(created_time), label = TRUE, abbr = FALSE),
           date = paste0(month, ' ', format(created_time,"%d"), ', ', format(created_time,"%Y")),
           icon = '') %>% 
    mutate(across(lifetime.impressions:engagementRate, ~ as.numeric(.x))) %>% 
    filter(!is.na(lifetime.impressions)) %>% 
    mutate(account = '')
  
  for(i in 1:nrow(posts)){
    
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
    
    link <- posts[i, 'perma_link']
    
    if(grepl('twitter.com/RMI_Industries', link)){
      posts[i, 'account'] <- 'RMI Industries'
    } else if(grepl('twitter.com/RMIPolicy', link)){
      posts[i, 'account'] <- 'RMI Policy'
    } else if(grepl('twitter.com/RMIBuildings', link)){
      posts[i, 'account'] <- 'RMI Buildings'
    } else if(grepl('twitter.com/RMICaribbean|www.facebook.com/101650939303293', link)){
      posts[i, 'account'] <- 'RMI Caribbean'
    } else if(grepl('twitter.com/RMIEmissions', link)){
      posts[i, 'account'] <- 'RMI Emissions'
    } else if(grepl('twitter.com/RMIAfrica', link)){
      posts[i, 'account'] <- 'RMI Africa'
    } else if(grepl('twitter.com/RockyMtnInst|www.facebook.com/344520634375161|www.instagram.com|linkedin.com|344046974422527', link) & linkedin == 'FALSE'){
      posts[i, 'account'] <- 'RMI Brand'
    } else if(grepl('twitter.com/RMIElectricity', link)){
      posts[i, 'account'] <- 'RMI Electricity'
    } else if(grepl('twitter.com/ClimateAlignmnt', link)){
      posts[i, 'account'] <- 'CCAF'
    } else if(grepl('https://twitter.com/CFANadvisors', link)){
      posts[i, 'account'] <- 'CFAN'
    } else if(grepl('https://twitter.com/AmoryLovins', link)){
      posts[i, 'account'] <- 'Amory Lovins'
    } else if(grepl('https://twitter.com/KingsmillBond', link)){
      posts[i, 'account'] <- 'Kingsmill Bond'
    } else if(grepl('https://twitter.com/Jon_Creyts', link)){
      posts[i, 'account'] <- 'Jon Creyts'
    }
    
    if(linkedin == 'Buildings'){
      posts[i, 'account'] <- 'RMI Buildings'
    } else if(linkedin == 'Transportation'){
      posts[i, 'account'] <- 'RMI Transportation'
    } else if(linkedin == 'CFAN'){
      posts[i, 'account'] <- 'CFAN'
    } else if(linkedin == 'CCAF'){
      posts[i, 'account'] <- 'CCAF'
    }
  }
  
  if(type == 'tagged'){
    posts <- posts %>% 
      mutate(icon = as.numeric(icon)) %>% 
      select(created_time, account, post_type, icon, id, text, impressions = lifetime.impressions, 
             engagements = lifetime.engagements, engagementRate, postClicks = lifetime.post_content_clicks,
             shares = lifetime.shares_count, perma_link) %>% 
      filter(grepl('LinkedIn|Twitter|Facebook|Instagram', post_type))
  } else {
    posts <- posts %>% 
      mutate(icon = as.numeric(icon)) %>% 
      select(created_time, account, post_type, icon, text, impressions = lifetime.impressions, 
             engagements = lifetime.engagements, engagementRate, postClicks = lifetime.post_content_clicks,
             shares = lifetime.shares_count, perma_link) %>% 
      filter(grepl('LinkedIn|Twitter|Facebook|Instagram', post_type))
  } 
  
  return(posts)
}

## get linkedin program account posts
getPosts <- function(ids, type){
  
  APT <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', 
                    lifetime.impressions = '', lifetime.post_content_clicks = '', 
                    lifetime.engagements = '', lifetime.shares_count = '', id = '')[0, ]
  
  for(i in 1:100){
    stats4 <- sproutPostRequest(i, paste0("created_time.in(2023-04-01T00:00:00..", currentDate, "T23:59:59)"), profileIDs = ids) 
    if(is.null(stats4)){ break }
    
    # data frame - posts with all tags
    stats4 <- stats4 %>% unnest(tags)
    APT <- APT %>% rbind(stats4)
    
  }
  
  return(APT)
  
}

## get and bind all posts from program accounts
getLIProgramPosts <- function(type){
  LICFAN <- cleanDF(getPosts('(5381251)', type), type = type, linkedin = 'CFAN')
  LICCAF <- cleanDF(getPosts('(5403265)', type), type = type, linkedin = 'CCAF')
  LIBUILD <- cleanDF(getPosts('(5541628)', type), type = type, linkedin = 'Buildings')
  LITRANSPORT <- cleanDF(getPosts('(5635317)', type), type = type, linkedin = 'Transportation')
  
  LI <- LICFAN %>% rbind(LICCAF) %>% rbind(LIBUILD) %>% rbind(LITRANSPORT)
  return(LI)
}

### 

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
  stats4 <- sproutPostRequest(i, 
                              dateRange = paste0("created_time.in(", '2023-01-01', "T00:00:00..", currentDate, "T23:59:59)"),
                              profileIDs = '(3244287, 2528134, 2528107, 2528104, 3354378, 4145669, 4400432, 4613890, 5083459, 5097954, 5098045, 5251820, 5334423, 5403312, 3246632, 5403593, 5403597)') 
  if(is.null(stats4)){ break }
  
  # unnest tags
  stats4 <- stats4 %>% unnest(tags)
  # bind query to data frame
  allPostsTags <- allPostsTags %>% rbind(stats4)
}

# get all posts from linkedIn program channels
linkedInTagged <- getLIProgramPosts('tagged')

# clean all posts and bind linkedin program posts
taggedPosts <- cleanDF(allPostsTags, 'tagged') %>% 
  rbind(linkedInTagged)

### get averages

# create data frames
posts1YR <- data.frame(created_time = '', post_type = '', text = '', perma_link = '',
                       lifetime.impressions = '', lifetime.post_content_clicks = '',
                       lifetime.engagements = '', lifetime.shares_count = '')[0, ]

# get all posts from social channels over the past year
for(i in 1:300){
  stats4 <- sproutPostRequest(i, 
                              dateRange = paste0("created_time.in(", oneYearAgo, "T00:00:00..", currentDate, "T23:59:59)"),
                              profileIDs = '(2528104, 2528107, 2528134, 3244287)',
                              AVG = TRUE)
  if(is.null(stats4)){ break }
  
  posts1YR <- posts1YR %>% rbind(stats4)
}

posts1YRaverage <- cleanDF(posts1YR, 'all') %>%
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




### REFERRALS
## get campaign key
referralSites <- read_sheet('https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843')

referrals <- referralSites %>% mutate(rows = '')
referrals[, 'rows'] <- as.numeric(rownames(referrals)) + 1


referrals <- referrals %>% 
  mutate(website = paste0('https://www.', media),
         title = gs4_formula(paste0('=HYPERLINK(B', rows, ', IMPORTXML(B', rows, ', "//head//title") )')))

write_sheet(referrals, ss = 'https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843', sheet = 'Referrals')

referralSites <- read_sheet('https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843')

referrals <- referralSites %>% 
  mutate(title = sub('( -| —| \\|)(.*)', '', title),
         title = ifelse(grepl('^Home$|N/A', title), '', title)) 
  
write_sheet(referrals, ss = 'https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843', sheet = 'Referrals2')



referralSites <- read_sheet('https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843', sheet = 'All Referral Sites')

pageTraffic <- ga_data(
  rmiPropertyID,
  metrics = c("sessions"),
  dimensions = c("sessionSource", "sessionMedium"),
  date_range = dates1,
  limit = -1
) %>% 
  filter(sessionMedium == 'referral') %>% 
  arrange(-sessions) 

ref <- pageTraffic %>% 
  inner_join(select(referralSites, c(media = title, domain, mediaType = category, mediaSubtype = category2)), by = c('sessionSource' = 'domain')) %>% 
  mutate(mediaSubtype = str_to_title(mediaSubtype)) %>% 
  filter(!grepl('Recruiting', mediaType))

write_sheet(ref, 'https://docs.google.com/spreadsheets/d/1DaP3VT4f53VuY7lcXzEbxZsfogz2rIwWYbi4OCNwePs/edit#gid=1192846843', sheet = 'All Referral Sites')



# newSitePages <- ga_data(
#   property_id,
#   metrics = c('screenPageViews', "totalUsers", "userEngagementDuration"),
#   dimensions = c("pageTitle"),
#   date_range = dates1,
#   limit = -1
# ) %>% 
#   filter(screenPageViews > 500) %>% 
#   mutate(engagementDuration = userEngagementDuration / totalUsers,
#          sec = round(engagementDuration %% 60, 0),
#          sec = ifelse(sec < 10, paste0('0', sec), sec),
#          min = (engagementDuration / 60) |> floor(),
#          avgEngagementDuration = paste0(min, ':', sec)) %>% 
#   select(pageTitle, screenPageViews, totalUsers, engagementDuration, avgEngagementDuration) %>% 
#   mutate(pageType = 'New Website',
#          icon = 5)
# 
# pages <- newSitePages$pageTitle

## look for content group tag - not set up yet
# contentGroup <- ga_data(
#   property_id,
#   metrics = c("sessions"),
#   dimensions = c('pageTitle', 'pagePath', 'campaignId', 'campaignName'),
#   date_range = dates1,
#   limit = -1
# ) %>% 
#   filter(contentGroup != "(not set)") %>%
#   filter(pageTitle != '')
# 
# pages <- contentGroup$pageTitle

## OCI+ Campaign
# pages <- c('Top Strategies to Cut Dangerous Methane Emissions from Landfills - RMI',
#           'OCI+ Update: Tackling Methane in the Oil and Gas Sector - RMI',
#           'Clean Energy 101: Methane-Detecting Satellites - RMI',
#           'Waste Methane 101: Driving Emissions Reductions from Landfills - RMI',
#           'Key Strategies for Mitigating Methane Emissions from Municipal Solid Waste - RMI',
#           'Know Your Oil and Gas - RMI',
#           'Intel from Above: Spotting Methane Super-Emitters with Satellites - RMI')




# mutate(AccountType = ifelse(!is.na(AccountType), AccountType, 
#                             ifelse(grepl('\\.gov|state.co.us|state.mn.us', Domain)|AccountType == 'Government', 'Government', 
#                                    ifelse(grepl('org$', Domain) & !grepl('rmi\\.org|third-derivative', Domain), 'Organization', 
#                                           ifelse(grepl('edu$', Domain), 'Academic', 
#                                                  ifelse(grepl('Household', Account), 'Household', AccountType))))))
# mutate(Audience1 = ifelse(level == 'FEDERAL', 'National Gov.',
#                           ifelse(level == 'STATE'|grepl('state of|commonwealth of', tolower(Account)), 'State Gov.',
#                                  ifelse(level == 'LOCAL'|level == 'COUNTY'|grepl('city of|county of', tolower(Account)), 'Local Gov.',
#                                         ifelse(level == 'INTERNATIONAL', 'International Gov.', ''))))) %>% 
# left_join(select(powerGenerators, c(Account, type)), by = c('Account')) %>%
# mutate(Audience1 = ifelse(!is.na(type) & is.na(Audience1), type, Audience1)) %>% 
# mutate(Audience1 = ifelse((grepl(audienceDomainsAccounts[1, 'multilateralDomains'], Domain)|grepl(audienceDomainsAccounts[1, 'multilateralAccounts'], Account)), 'Multilateral Institution',
#                           ifelse((grepl(audienceDomainsAccounts[1, 'NGODomains'], Domain)|grepl(audienceDomainsAccounts[1, 'NGOAccounts'], Account)), 'NGO',
#                                  ifelse((grepl(audienceDomainsAccounts[1, 'financialDomains'], Domain)|grepl(audienceDomainsAccounts[1, 'financialAccounts'], Account)), 'Financial Entity', Audience1)))) %>% 
# mutate(Audience1 = ifelse(grepl('Corporate', AccountType) & (Audience1 == ''|is.na(Audience1)), 'Other Corporate', Audience1),
#        Audience1 = ifelse(grepl('Foundation', AccountType), 'Foundation', Audience1),
#        Audience1 = ifelse(grepl('Academic', AccountType) & (Account != '' | is.na(Account) | Account != 'Unknown'), 'Academic', Audience1),
#        Audience1 = ifelse((is.na(Audience1) | Audience1 == '') & grepl('Foundation', AccountType), 'Foundation', Audience1),
#        Account = ifelse(is.na(Account), 'Unknown', Account),
#        Audience1 = ifelse(Account == 'Unknown'|Audience1 == ''|is.na(Audience1), 'N/A', Audience1),
#        Audience2 = ifelse(grepl('Gov', Audience1), 'Government', Audience1),
#        Account = ifelse(grepl('RMI', Account), 'Household', Account),
#        Account = ifelse(Account == 'Unknown' & !is.na(govName), govName, Account),
#        Account = ifelse(is.na(Account), 'Unknown', Account),
#        Account = ifelse(grepl('Household', Account), 'Household', Account)) 



# get list of all reports
reports <- campaign_list %>% 
  filter(Type == 'Report') %>% 
  filter(grepl('^RP', Name)) %>% 
  filter(Members > 10)

# get list of all events
events <- campaign_list %>% 
  filter(grepl('Event|Training|Workshop', Type) & Type != 'Development Event') %>% 
  filter(Members > 0)


###

getFields <- function(object, inlineHelpText = TRUE, calculatedFormula = TRUE) {
  
  df_fields <- sf_describe_object_fields(object) %>% 
    relocate(label, .before = 1) %>% 
    relocate(name, .after = 1)
  
  if(inlineHelpText == TRUE){ df_fields <- df_fields %>% relocate(inlineHelpText, .after = 2) }  
  
  if(calculatedFormula == TRUE){ df_fields <- df_fields %>% relocate(calculatedFormula, .after = 3) } 
  
  return(df_fields)
  
}

listEmailFields <- getFields('ListEmail', calculatedFormula = FALSE, inlineHelpText = FALSE)
###
campaignMemberFields <- getFields('CampaignMember', FALSE)

# get campaign members
my_soql <- sprintf("SELECT Id,
                           Name,
                           CreatedDate,
                           Subject,
                           HtmlBody,
                           TextBody,
                           FromName,
                           Status,
                           Type,
                           HasAttachment,
                           TotalOpens,
                           UniqueOpens,
                           CampaignId,
                           TotalDelivered,
                           IsTracked

                      FROM ListEmail
                      LIMIT 1000")

listEmails <- sf_query(my_soql, 'ListEmail', api_type = "Bulk 1.0") %>% 
  filter(Status == 'Sent')


campaignIdList <- c('7016f0000023VTgAAM')

my_soql <- sprintf("SELECT CampaignId,
                           Name,
                           Status,
                           HasResponded,
                           ContactId,
                           LeadId,
                           CreatedDate,
                           Account_Name_text__c,
                           Lifetime_Giving__c

                    FROM CampaignMember
                    WHERE CampaignId in ('%s')",
                   paste0(campaignIdList, collapse = "','"))

campaignMembers <- sf_query(my_soql, 'CampaignMember', api_type = "Bulk 1.0") 

ListtEmailIdList <- c('0XB6f000000g5FEGAY')


my_soql <- sprintf("SELECT Name,
                           CreatedDate,
                           ListEmailId,
                           RecipientId	


                    FROM ListEmailIndividualRecipient
                    WHERE ListEmailId in ('%s')",
                   paste0(ListtEmailIdList, collapse = "','"))

liIr <- sf_query(my_soql, 'ListEmailIndividualRecipient', api_type = "Bulk 1.0") 

liIr2 <- liIr %>% 
  left_join(prospects, by = c('RecipientId' = 'Id'))
