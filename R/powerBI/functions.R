
#############
#' FUNCTIONS #
#############

#' function that 1. binds the Campaign ID to all final dataframes and 2. pushes the data to a google sheet
#' if data already exists in sheet, df is bound to existing data and pushed
pushData <- function(df, sheetName){
  
  #' bind campaign ID
  df <- df %>% 
    mutate(CAMPAIGN_ID = campaignID) %>% 
    relocate(CAMPAIGN_ID, .before = 1)
  
  tryCatch({ 
    
    existingData <- read_sheet(ss = ss, sheet = sheetName) %>% 
      rbind(df)
    
    #' if development mode is on, overwrite data in sheet
    if(mode == 'development'){
      write_sheet(df, ss = ss, sheet = sheetName) 
      return(df)
    } else {
      write_sheet(existingData, ss = ss, sheet = sheetName) 
      return(existingData)
    }
    
    #' catch error when the sheet does not already exist
  }, error = function(e) { 
    write_sheet(df, ss = ss, sheet = sheetName) 

    return(df) 
  })

}

#### GOOGLE ANALYTICS ####

#' get page title and content type by web scraping page URLs
getPageData <- function(df){
  
  for(i in 1:nrow(df)){
    
    url <- as.character(df[i, 'pageURL'])
    
    #' get page titles
    tryCatch( { 
      url_tb <- url %>%
        read_html() %>% 
        html_nodes('head > title') %>% 
        html_text() %>% 
        as.data.frame() %>% 
        rename(title = 1) 
      
      df[i, 'pageTitle'] <- url_tb[1, 'title']
      
    }, error = function(e){
      df[i, 'pageTitle'] <- NA
    })
    
    #' get contet type from page metadata
    if(df[i, 'site'] == 'rmi.org'){
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
        
        df[i, 'metadata'] <- url_tb[2, 'keywords']
        
      }, error = function(e){
        df[i, 'metadata'] <- NA
      })
    } else {
      #' categorize as 'New Website' if no metadata is detected
      df[i, 'metadata'] <- NA
      df[i, 'pageType'] <- 'New Website'
    }
    
  }
  
  #' categorize as 'Article' or 'Report' if these terms are detected in the metadata
  df <- df %>% 
    mutate(pageType = ifelse(grepl('article', tolower(metadata)), 'Article',
                             ifelse(grepl('report', tolower(metadata)), 'Report', pageType)),
           icon = ifelse(grepl('article', tolower(metadata)), 4,
                         ifelse(grepl('report', tolower(metadata)), 1, 5))) %>% 
    distinct(pageTitle, .keep_all = TRUE)
  
}

#' get web traffic metrics for all pages
getPageMetrics <- function(propertyID, pages){
  campaignPages <- ga_data(
    propertyID,
    metrics = c('screenPageViews', "totalUsers", "userEngagementDuration"),
    dimensions = c("pageTitle"),
    date_range = dateRangeGA,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    #' calculate average engagement duration from userEngagementDuration and convert seconds to mm:ss format
    mutate(engagementDuration = userEngagementDuration / totalUsers,
           sec = round(engagementDuration %% 60, 0),
           min = (engagementDuration / 60) |> floor(),
           avgEngagementDuration = paste0(min, ':', ifelse(nchar(sec) == 1, paste0('0', sec), sec))) %>% 
    select(pageTitle, screenPageViews, totalUsers, engagementDuration, avgEngagementDuration) %>% 
    left_join(select(pageData, c(pageTitle, pageType, icon)), by = c('pageTitle')) %>% 
    #' remove " - RMI" from end of page titles
    mutate(pageTitle = gsub(' - RMI', '', pageTitle))
  
  return(campaignPages)
}

#' correct GA acquisition attribution for social media and email channels 
correctTraffic <- function(df, type){
  if(type == 'session'){
    df <- df %>% 
      rename(medium = sessionMedium,
             source = sessionSource,
             defaultChannelGroup = sessionDefaultChannelGroup)
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

#' get page traffic (#' sessions) driven by social media channels
getTrafficSocial <- function(propertyID, pages, site = 'rmi.org'){
  
  aquisitionSocial <- ga_data(
    propertyID,
    metrics = c("sessions", "screenPageViews"),
    dimensions = c("pageTitle", "sessionSource", "sessionMedium", 'pageReferrer', 'sessionDefaultChannelGroup'),
    date_range = dateRangeGA,
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

#' get page views broken down by country and region
getTrafficGeography <- function(propertyID, pages, site = 'rmi.org'){
  
  trafficByRegion <- ga_data(
    propertyID,
    metrics = c('screenPageViews'),
    dimensions = c("pageTitle", 'region', 'country'),
    date_range = dateRangeGA,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    #' filter out regions where page views < 5
    filter(screenPageViews > 4) %>% 
    arrange(pageTitle) %>% 
    dplyr::rename('Region Page Views' = screenPageViews) %>% 
    mutate(pageTitle = gsub(' - RMI', '', pageTitle),
           site = site, 
           dashboardCampaign = campaignID)
  
  return(trafficByRegion)
}

#' get sessions and conversions attributions for acquisition channels (organic, email, social, paid ads, etc.)
#' sessions and conversions use different dimensions so make separate calls for each then bind rows
getAcquisition <- function(propertyID, pages, site = 'rmi.org'){
  
  #' 1) get sessions
  aquisitionSessions <- ga_data(
    propertyID,
    metrics = c("sessions"),
    dimensions = c("pageTitle", "sessionSource", "sessionMedium", "pageReferrer", 'sessionDefaultChannelGroup'),
    date_range = dateRangeGA,
    dim_filters = ga_data_filter("pageTitle" == pages),
    limit = -1
  ) %>% 
    arrange(pageTitle) 
  
  acquisition <- correctTraffic(aquisitionSessions, 'session') %>% 
    group_by(pageTitle, defaultChannelGroup) %>% 
    summarize(Sessions = sum(sessions))
  
  if(site == 'rmi.org'){
    
    #' 2) get conversions
    aquisitionConversions <- ga_data(
      propertyID,
      metrics = c('conversions:form_submit', 'conversions:file_download'),
      dimensions = c("pageTitle", "source", "medium", 'pageReferrer', 'defaultChannelGroup'),
      date_range = dateRangeGA,
      dim_filters = ga_data_filter("pageTitle" == pages),
      limit = -1
    ) %>% 
      select(pageTitle, source, medium, pageReferrer, defaultChannelGroup, 
             form_submit = 'conversions:form_submit', download = 'conversions:file_download') %>% 
      arrange(pageTitle)
    
    aquisitionConversions <- correctTraffic(aquisitionConversions, 'conversion') %>% 
      group_by(pageTitle, defaultChannelGroup) %>% 
      summarize('Downloads' = sum(download),
                       'Form Submissions' = sum(form_submit)) 
    
    #' 3) bind sessions + conversions 
    acquisition <- acquisition %>% 
      left_join(aquisitionConversions, by = c('pageTitle', 'defaultChannelGroup')) 
    
  }
  
  acquisition <- acquisition %>% 
    mutate(site = site)
  
  return(acquisition)
  
}

#' get page traffic (#sessions) driven by referral sources that have been identified as “Media” 
#' these sources are defined in the referralSites file
getReferrals <- function(propertyID, pages, site = 'rmi.org'){
  
  referrals <- ga_data(
    propertyID,
    metrics = c("sessions"),
    dimensions = c("pageTitle", "sessionSource", "sessionMedium", "pageReferrer"),
    date_range = dateRangeGA,
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
    mutate(site = site,
           pageTitle = gsub(' - RMI', '', pageTitle))
  
  return(referrals)
  
}

#### NEWSLETTERS ####

##' get newsletter stats

#' get all Spark and Market Catalyst newsletters
getAllEmailStats <- function(){
  
  #' get Spark newsletters
  emailStatsSpark <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Spark Stats (Unformatted)') %>% 
    mutate(date = as.Date(date))
  
  #' get Market Catalyst newsletters
  emailStatsPEM <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Market Catalyst Stats (Unformatted)') %>% 
    mutate(date = as.Date(date))
  
  #' bind 
  allEmailStats <- emailStatsSpark %>% 
    plyr::rbind.fill(emailStatsPEM) %>% 
    mutate(date = as.Date(date),
           #' change name format
           name = ifelse(grepl('Spark', name), paste0(date, ': Spark'), paste0(date, ':', sub('(.*)-[0-9]{2}', '', name))),
           id = as.numeric(id))
  
  return(allEmailStats)
}

#' filter all newsletters data frame by page URLs
getCampaignEmails <- function(pageURLs){
  
  #' filter each of the 3 "story_url" columns for campaign web page URLs
  df1 <- allEmailStats %>% select(c('id':'COR_S1')) %>% 
    rename(story_url = url_1,
           story_title = title_1,
           story_clicks = clicks_1, 
           story_COR = COR_S1)
  
  df2 <- allEmailStats %>% select(c('id':'plaintext_rate', 'url_2':'COR_S2'))
  names(df2) <- names(df1)
  
  df3 <- allEmailStats %>% select(c('id':'plaintext_rate', 'url_3':'COR_S3'))
  names(df3) <- names(df1)
  
  #' bind matches
  allStoryStats <- df1 %>% 
    rbind(df2) %>% 
    rbind(df3) %>% 
    filter(grepl(paste(pageURLs, collapse = '|'), story_url)) %>% 
    mutate(date = as.Date(date),
           story_title = gsub(' - RMI', '', story_title))
  
  #' order by date and keep relevant columns
  allStoryStats <- allStoryStats[rev(order(allStoryStats$date)),] %>% 
    select(id, name, date, delivered_ = delivered, unique_opens, open_rate, unique_clicks,
           unique_CTR, UCTRvsAvg, story_url, story_title, story_clicks, story_COR)
  
  return(allStoryStats)
}

##' get individual newsletter clicks - for Salesforce campaign member data frame

#' make a GET call through Pardot API
get <- function(url, header) {
  request <- GET(url, add_headers(.headers = header))
  response <- jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8"))
}

#' get individual link clicks from a list email
#' - limit of 200 rows returned are returned at a time so use the nextDate field to get the next page of results
#' - break loop when query returns a data frame with < 200 rows
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

#' apply getBatchClicks to a list of emailIDs
#' - returns df with all link clicks attached to those emails
getProspectClicks <- function(df){
  clicksTotal <- data.frame(id = '', prospect_id = '', url = '', list_email_id = '', email_template_id = '', created_at = '')[0,]
  
  for(i in 1:nrow(df)){
    clicks <- getBatchClicks(i, df)
    clicksTotal <- clicksTotal %>% rbind(clicks)
  }
  
  return(clicksTotal)
}

#### SALESFORCE ####

#' get all prospects (all contacts and leads with Pardot Activity)
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
    #' remove instances of duplicate Pardot URLs (which contains a prosepct's Pardot ID) where one is both a Lead and Contact
    #' by using anti_join to get rid of all Leads if a Contact has the same Pardot URL
    anti_join(allContacts, by = 'Pardot_URL')
  
  #' bind contacts and leads
  prospects <- allContacts %>% 
    plyr::rbind.fill(allLeads) %>% 
    filter(!is.na(Email))
  
  #' identify and remove duplicates
  dup <- prospects[duplicated(prospects[,c("Pardot_URL")]) | duplicated(prospects[,c("Pardot_URL")], fromLast = TRUE), ] 
  
  #' from list of duplicated, filter for records where there is no account name
  dup1 <- dup %>% 
    filter(grepl('unknown|not provided|contacts created', tolower(Account))|Account == ''|is.na(Account)) %>% 
    distinct(Pardot_URL, .keep_all = TRUE)
  
  #' remove these prospects
  prospects_keep <- prospects %>% 
    anti_join(dup1) 
  
  #' finally, apply distinct() to remove all duplicate instances 
  unique_prospects <- prospects_keep %>% 
    distinct(Pardot_URL, .keep_all = TRUE)
  
  #' final df contains 188,615 unique prospects
  return(unique_prospects)
}

#' get all accounts from SF
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

#' functions to get reports and events

#' get list of all campaigns created after 2022-01-01
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

#' for a list of campaign IDs, get campaign members and join to contact/lead/account info
getCampaignMembers <- function(campaignIdList, campaignType) {
  
  #' get campaign members
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
  
  #' get info for contacts in campaign
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
  
  #' get info for leads in campaign
  my_soql <- sprintf("SELECT Id,  Name, Company, Email, pi__url__c, pi__score__c, pi__last_activity__c

                      FROM Lead
                      WHERE Id in ('%s')
                      ORDER BY pi__last_activity__c DESC NULLS LAST",
                     paste0(campaignLeads$LeadId, collapse = "','"))
  
  campaignLeadsQuery <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c) %>% 
    mutate(RecordType = 'Lead') 
  
  #' get info for accounts that match the AccountID field pulled from contacts query
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
  
  #' join
  campaignContactsQuery <- campaignContactsQuery %>% 
    left_join(select(campaignContactAccounts, -Account), by = c('AccountId'))
  
  campaignLeadsQuery <- campaignLeadsQuery %>% 
    left_join(campaignContactAccounts, by = c('Account')) 
  
  contactsLeads <- campaignContactsQuery %>%
    plyr::rbind.fill(campaignLeadsQuery) 
  
  #' join to campaign members df
  CampaignMembers <- campaign_members %>% 
    mutate(Id = ifelse(is.na(ContactId), LeadId, ContactId)) %>% 
    select(CampaignId, CampaignName, Name, Id, Status, CreatedDate) %>% 
    left_join(contactsLeads, by = c('Id', 'Name'))
  
  #' specify campaign type
  if(campaignType == 'Event'){
    
    CampaignMembers <- CampaignMembers %>% 
      mutate(EngagementType = 'Event',
             #' rename status categories and filter for rows where status is "registered" or "attended"
             Status = ifelse(grepl('Register', Status), 'Registered (Did Not Attend)', Status)) %>% 
      filter(grepl('Register|Attended', Status))
    
  } else if (campaignType == 'Report'){
    
    CampaignMembers <- CampaignMembers %>% 
      mutate(EngagementType = 'Report Download') 
    
  }
  
  CampaignMembers <- CampaignMembers %>% 
    #' get domain from email
    mutate(Domain = sub("(.*)\\@", "", Email),
           EngagementDate = as.Date(CreatedDate, format = "%Y-%m-%d")) %>% 
    #' select and reorder columns
    select(CampaignName, EngagementType, Id, RecordType, Status, EngagementDate, Name, Email, Domain, Account, AccountType, Industry, 
           TotalGiving, NumOpenOpps, Pardot_Score, Pardot_URL, Giving_Circle, Last_Gift, AccountId)
  
  return(CampaignMembers)
  
}

##' get campaigns and clean 
cleanCampaignDF <- function(df){
  
  df <- df %>% 
    filter(!is.na(Domain)) %>% 
    distinct(Id, CampaignName, .keep_all = TRUE) %>% 
    mutate(
      DonorType = 
        #' define donor type
        case_when(
           !is.na(Giving_Circle) ~ Giving_Circle,
           !is.na(Last_Gift) ~ 'Donor',
           .default = NA
           ),
      #' define icon for Power BI
      Icon = 
        case_when(
           EngagementType == 'Report Download' ~ 1,
           EngagementType == 'Event' ~ 2, 
           EngagementType == 'Newsletter' ~ 3, 
           .default = NA
           ),
      #' get Pardot ID from Pardot URL
      Pardot_ID = sub("(.*)=", "", Pardot_URL)) %>% 
    #' join all accounts based on email domain
    #' use account name, account type, and industry if these fields are empty
    left_join(select(all_accounts, c(AccountsName = Account, Domain = AccountDomain, AccountsIndustry = Industry, AccountType2 = AccountType)), by = c('Domain')) %>% 
    mutate(Account = ifelse(grepl('unknown|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account),
           Industry = ifelse(Account == 'Unknown' & !is.na(Industry), AccountsIndustry, Industry),
           AccountType = ifelse(Account == 'Unknown' | (is.na(AccountType) & !is.na(AccountType2)), AccountType2, AccountType)) %>% 
    mutate(Account = ifelse(Account == 'Unknown' & !is.na(AccountsName), AccountsName, Account),
           Account = ifelse(is.na(Account), 'Unknown', Account)) %>% 
    #' join info for government accounts using domain info specified in the govDomains file
    left_join(select(govDomains, c(govDomain = domain, level, govName = name)), by = c('Domain' = 'govDomain')) %>%
    mutate(
      Account = 
        case_when(
          is.na(Account) ~ 'Unknown',
          #' label all Household accounts as Household (remove individual names)
          #' label all RMI accounts as Household
          grepl('Household', Account) | Account == 'RMI' ~ 'Household',
          #' use account name matches from govDomains file if account name is unknown
          Account == 'Unknown' & !is.na(govName) ~ govName,
          .default = Account
        ),
      #' create audience grouping to categorize accounts that fall under Government, NGO, Multilaterals, 
      #' Financial Entities, Utilities/Power Generators, Other Corporate, Academic, or Foundation
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
          (grepl('Academic', AccountType)) & (Account != '' | is.na(Account) | Account != 'Unknown') ~ 'Academic',
          Account == 'Unknown'|is.na(Audience1) ~ 'N/A',
          .default = Audience1
        ),
      Audience2 = ifelse(grepl('Gov', Audience1), 'Government', Audience1)
    ) %>% 
    #' create True or False columns for each of the following variables to make 
    #' data manipulation easier on Power BI
    mutate(DownloadTF = ifelse(grepl('Download', EngagementType), 1, NA),
           EventTF = ifelse(grepl('Attended', Status), 1, NA),
           EmailClickTF = ifelse(grepl('Newsletter', EngagementType), 1, NA),
           GivingCircleTF = ifelse(DonorType == 'Solutions Council'|DonorType == 'Innovators Circle', 1, NA),
           SolutionsCouncilTF = ifelse(DonorType == 'Solutions Council', 1, NA),
           InnovatorsCircleTF = ifelse(DonorType == 'Innovators Circle', 1, NA),
           OpenOppTF = ifelse(NumOpenOpps == ''|NumOpenOpps == 0|is.na(NumOpenOpps), NA, 1),
           DonorTF = ifelse(DonorType == ''|is.na(DonorType), NA, 1),
           LastGift = as.numeric(Last_Gift),
           LapsedDonorsTF = ifelse((LastGift > 549 & LastGift < 1825), 1, NA),
           Engagements = ifelse(!grepl('Registered', Status), 1, NA)) %>% 
    #' select and reorder columns
    select(CampaignName, EngagementType, Icon, Id, Status, EngagementDate, Domain, Email, 
           DonorType, AccountId, Account, AccountType, Audience1, Audience2, Industry, TotalGiving, Name, Pardot_Score,
           Pardot_URL, Pardot_ID, GivingCircleTF, SolutionsCouncilTF, InnovatorsCircleTF, OpenOppTF, DonorTF, LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) 
  
  return(df)
}

##' get campaign member data for reports (SF)
getSalesforceReports <- function(){
  
  message('getting reports')
  campaignMembersReports <- getCampaignMembers(campaignReports$reportID, 'Report') 
  campaignMembersReports <- cleanCampaignDF(campaignMembersReports)
  
  return(campaignMembersReports)
  
}

##' get campaign member data for events (SF)
getSalesforceEvents <- function(){
  
  message('getting events')
  campaignMembersEvents <- getCampaignMembers(campaignEvents$eventID, 'Event')
  campaignMembersEvents <- cleanCampaignDF(campaignMembersEvents)
  
  return(campaignMembersEvents)
  
}

##' get campaign member data for email clicks (Pardot)
getCampaignNewsletters <- function(){
  
  message('getting newsletter clicks')
  
  #' get email IDs and story URLs from newsletters in this campaign
  emailIDs <- data.frame(id = unique(campaignNewsletters$id))
  storyLinks <- unique(campaignNewsletters$story_url)
  
  #' get clicks for these newsletters
  clicksAll <- getProspectClicks(emailIDs)
  
  #' clean and filter links for clicks on story URLs only
  linkClicks <- clicksAll %>% 
    mutate(url = sub('\\?(.*)', '', url)) %>% 
    filter(grepl(paste(storyLinks, collapse = '|'), url))
  
  #' clean clicks df
  clicksByProspect <- select(linkClicks, c(emailId = list_email_id, Pardot_URL = prospect_id, url, created_at)) %>% 
    #' pardot call doesn't supply contact IDs or Account IDs so join using Pardot IDs embedded in Pardot URLs
    mutate(Pardot_URL = paste0("http://pi.pardot.com/prospect/read?id=", as.character(Pardot_URL)),
           EngagementDate = as.Date(created_at, format="%Y-%m-%d")) %>% 
    left_join(prospects, by = c('Pardot_URL')) %>% 
    left_join(select(allEmailStats, c(emailId = id, CampaignName = name)), by = 'emailId') %>% 
    mutate(Status = 'Email',
           EngagementType = 'Newsletter') %>% 
    mutate(Account = ifelse(grepl('unknown|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account)) %>% 
    #' left join all accounts using account name
    left_join(select(accountsUnique, c(AccountsName = Account, AccountDomain, Industry, AccountType, TotalGiving, 
                                       NumGifts, NumOpenOpps, AccountId2 = AccountId)), 
              by = c('Account' = 'AccountsName')) %>% 
    select(CampaignName, EngagementType, Id, RecordType, Status, EngagementDate, Name, Email, Domain, Account, AccountType, Industry,
           TotalGiving, NumOpenOpps, Pardot_Score, Pardot_URL, Giving_Circle, Last_Gift, AccountId)
  
  #' clean df and set audience groups
  campaignMembersEmail <- cleanCampaignDF(clicksByProspect)
  
  return(campaignMembersEmail)
}

#### Donations ####

#' get opportunities/donations submitted by all contacts in this campaign
#' 1. filter for donations that occurred after interacting with this campaign
#' 2. calculate attributed revenue

#' Donation Revenue Attribution:
#' Donation revenue is attributed if a contact engaged with a campaign component up to 9 months after making a donation. 
#' The full amount is attributed within the first 30 days of a donation. After this point, the value is discounted by 
#' a factor of 0.0041 (1/244 days) for each additional day.
#' If a donor engaged with multiple campaign components, the donation value is divided accordingly.


##' get opportunities
getOpportunities <- function(df){
  
  message('getting donations')
  allOpportunities <- data.frame(DonationID = '', DonationDate = '', DonationValue = '', Pardot_ID = '', EngagementDate = '',
                                 EngagementType = '', CampaignName = '', Pardot_Score = '',
                                 TotalGiving = '', DonorType = '')[0,]
  for(i in 1:nrow(df)){
    
    visitorActivityCall <- GET(paste0('https://pi.pardot.com/api/opportunity/version/4/do/query?format=json&prospect_id=', df[i, 'Pardot_ID']),
                               add_headers(.headers = header4))
    visitorActivity <- jsonlite::fromJSON(content(visitorActivityCall, as = "text", encoding = "UTF-8"))
    tryCatch( { 
      if(visitorActivity[["result"]][["total_results"]] != 0){
        donationHistory <- data.frame(
          DonationId = visitorActivity[["result"]][["opportunity"]][["id"]],
          DonationDate = visitorActivity[["result"]][["opportunity"]][["created_at"]],
          DonationValue = visitorActivity[["result"]][["opportunity"]][["value"]],
          DonationStatus = visitorActivity[["result"]][["opportunity"]][["status"]]
          ) %>% 
          filter(DonationStatus == 'Won') %>% 
          cbind(df[i, 'EngagementDate']) %>% 
          mutate(Pardot_ID = paste(df[i, 'Pardot_ID']),
                 Id = paste(df[i, 'Id']),
                 DonationDate = as.Date(DonationDate, format = "%Y-%m-%d"),
                 EngagementDate = as.Date(EngagementDate, format="%Y-%m-%d"),
                 EngagementType = paste(df[i, 'EngagementType']),
                 CampaignName = paste(df[i, 'CampaignName']),
                 Pardot_Score = paste(df[i, 'Pardot_Score']),
                 TotalGiving = paste(df[i, 'TotalGiving']),
                 DonorType = paste(df[i, 'DonorType'])) 
        allOpportunities <- allOpportunities %>% rbind(donationHistory)
      }
    }, error = function(e){ NA } )
    
  }
  return(allOpportunities)
}

#' calculate attributed donation value
getAttributedDonationValue <- function(df){
  
  if(nrow(df) > 0){
    
    #' get all donors in this campaign
    donors <- df %>% 
      select(Pardot_ID, Name, DonorType, EngagementType, CampaignName, EngagementDate, Id, Pardot_Score, TotalGiving, Account) %>% 
      distinct() %>% 
      filter(!is.na(DonorType))
    
    #' get donation history for these donors
    allOpps <- getOpportunities(donors) 
    
    #' select donations made after campaign
    donations <- allOpps %>% 
      filter(DonationDate > EngagementDate)
    
    #' find donations that touch multiple campaign components
    uniqueDonations <- donations %>% 
      group_by(DonationId) %>% 
      summarize(count = n())
    
    donations <- donations %>% 
      #' calculate attributed donation value
      mutate(DonationValue = as.numeric(DonationValue),
             TimeDifference = difftime(DonationDate, EngagementDate, units = "days"),
             TimeDifference = as.numeric(gsub("[^0-9.-]", "", TimeDifference)),
             TimeDifferenceAdjusted = ifelse(TimeDifference < 31, 1, TimeDifference),
             AttributtedValue = ifelse(DonationValue / (1/(1.0041 - 0.0041*TimeDifferenceAdjusted)) < 0, 0, DonationValue / (1/(1.0041 - 0.0041*TimeDifferenceAdjusted)))) %>% 
      relocate(DonationValue, .before = AttributtedValue) %>%  
      #' adjust attribution value for donations that attributed to multiple campaign components
      left_join(uniqueDonations) %>% 
      mutate(AttributtedValue = round(AttributtedValue/count, 1)) %>% 
      filter(AttributtedValue > 0) %>% 
      select(-TimeDifferenceAdjusted) 
    
    return(donations)
  } else {
    return( NA )
  }
  
}

#### SPROUT SOCIAL ####

#' metadata request
getMetadata <- function(url) {
  request <- GET(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                 add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

#' call for all other requests
getCall <- function(url, args) {
  request <- POST(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                  body = toJSON(args, auto_unbox = TRUE),
                  add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

#' post analytics request
sproutPostRequest <- function(page, dateRange, profileIDs, tagged = TRUE){
  
  #' set tagged == FALSE when making call to get all posts (returns distinct posts rather than duplicate posts if a post has multiple tags)
  #' explanation: some posts this far back weren't getting tagged so the internal.tags.id field would return an error
  if(tagged == TRUE) {
    fields <- c(
      "created_time",
      "perma_link",
      "text",
      "internal.tags.id",
      "post_type")
  } else {
    #' remove internal.tags.id field if tagged == FALSE
    fields <- c(
      "created_time",
      "perma_link",
      "text",
      "post_type")
  }
  
  #' define arguments
  args <- list(
    "fields" = fields,
    #' profile IDs are specific to an account (e.g. RMI Brand LinkedIn, RMI Buildings Twitter)
    #' you must supply the profile ID of each account you want to include in your request
    "filters" = c(paste0("customer_profile_id.eq", profileIDs),
                  dateRange),
    #' set post metrics
    "metrics" = c("lifetime.impressions", "lifetime.engagements", "lifetime.post_content_clicks", "lifetime.shares_count"), 
    "timezone" = "America/Denver",
    "page" = paste(page))
  
  #' make call
  getStats <- getCall(url = 'analytics/posts', args = args)
  
  #' call returns 50 posts at a time
  #' NULL indicates that you have reached the last page of results so make this call until the paging field returns NULL
  if(is.null(getStats[["paging"]])) {
    postStats <- NULL
  } else if(tagged == TRUE) {
    metrics <- getStats[["data"]][["metrics"]]
    internal <- getStats[["data"]][["internal"]]
    postStats <- getStats[["data"]] %>% 
      select(-c('metrics', 'internal')) %>% 
      cbind(metrics) %>% 
      cbind(internal)
  } else if(tagged == FALSE) {
    metrics <- getStats[["data"]][["metrics"]]
    postStats <- getStats[["data"]] %>% 
      select(-c('metrics')) %>% 
      cbind(metrics) 
  }
  
  return(postStats)
}

#' get all social media posts with tags
getAllSocialPosts <- function(){
  
  #' create data frame to store all posts
  allPostsTags <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', 
                             lifetime.impressions = '', lifetime.post_content_clicks = '', 
                             lifetime.engagements = '', lifetime.shares_count = '', id = '')[0, ]
  
  #' get all posts from social channels dating back to Jan 1, 2023
  #' note: 300 was chosen as an arbitrary ceiling. the call will end well before this point is reached.
  for(i in 1:300){
    postStats <- sproutPostRequest(i, 
                                   dateRange = paste0("created_time.in(", '2023-01-01', "T00:00:00..", currentDate, "T23:59:59)"),
                                   profileIDs = '(3244287, 2528134, 2528107, 2528104, 3354378, 4145669, 4400432, 4613890, 5083459, 5097954, 5098045, 5251820, 5334423, 5403312, 3246632, 5403593, 5403597)') 
    #' break loop if NULL value is returned (indicates last page reached)
    if(is.null(postStats)){ break }
    
    #' unnest tags
    postStats <- postStats %>% unnest(tags)
    #' bind query to data frame
    allPostsTags <- allPostsTags %>% rbind(postStats)
  }
  
  return(allPostsTags)
}

#' clean response
cleanPostDF <- function(df, type, linkedin = 'FALSE'){
  
  posts <- df %>% 
    mutate(engagementRate = round(as.numeric(lifetime.engagements)/as.numeric(lifetime.impressions), 3),
           created_time = as.Date(sub('T(.*)', '', created_time)),
           icon = '',
           account = '',
           across(lifetime.impressions:engagementRate, ~ as.numeric(.x))) %>% 
    filter(!is.na(lifetime.impressions)) 
  
  for(i in 1:nrow(posts)){
    
    #' rename post types
    #' define platform icon for Power BI
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
    
    #' identify accounts using post URLs
    #' explanation: the call doesn't return the account/profileID that made the post so use post links - perma_link field - to identify the account
    #' note: this works for all platforms except LinkedIn, which doesn't include account info in post URLs
    link <- posts[i, 'perma_link']
    
    if(grepl('twitter.com/RockyMtnInst|www.facebook.com/344520634375161|www.instagram.com|linkedin.com|344046974422527', link) & linkedin == 'FALSE'){
      posts[i, 'account'] <- 'RMI Brand'
    } else if(grepl('twitter.com/RMI_Industries', link)){
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
    } else if(grepl('twitter.com/RMIElectricity', link)){
      posts[i, 'account'] <- 'RMI Electricity'
    } else if(grepl('twitter.com/RMICities', link)){
      posts[i, 'account'] <- 'RMI Cities'
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
    
    #' identify LinkedIn program accounts after making separate calls for each program LinkedIn account
    #' after making each call, provide the account name as an argument using the 'linkedin' parameter
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
      left_join(select(tags, c(id = tag_id, tag_name = text)), by = 'id') %>% 
      select(created_time, account, post_type, icon, tag_id = id, tag_name, text, impressions = lifetime.impressions, 
             engagements = lifetime.engagements, engagementRate, postClicks = lifetime.post_content_clicks,
             shares = lifetime.shares_count, perma_link, text)
  } else {
    posts <- posts %>% 
      select(created_time, account, post_type, icon, impressions = lifetime.impressions, 
             engagements = lifetime.engagements, engagementRate, postClicks = lifetime.post_content_clicks,
             shares = lifetime.shares_count, perma_link, text) 
  } 
  
  posts <- posts %>% 
    mutate(icon = as.numeric(icon)) %>% 
    filter(grepl('Facebook|LinkedIn|Twitter|Instagram', post_type))
  
  return(posts)
}

#' make separate calls for LinkedIn program accounts - get all LinkedIn posts from these accounts 
#' if requesting tagged posts, call only works if earliest date >= April 2023
getPosts <- function(ids, type){
  
  AP <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', 
                   lifetime.impressions = '', lifetime.post_content_clicks = '', 
                   lifetime.engagements = '', lifetime.shares_count = '')[0, ]
  
  APT <- data.frame(created_time = '', post_type = '', text = '', perma_link = '', 
                    lifetime.impressions = '', lifetime.post_content_clicks = '', 
                    lifetime.engagements = '', lifetime.shares_count = '', id = '')[0, ]
  
  for(i in 1:100){
    if(type == 'tagged'){
      postStats <- sproutPostRequest(i, 
                                     paste0("created_time.in(2023-04-01T00:00:00..", currentDate, "T23:59:59)"), 
                                     profileIDs = ids, 
                                     tagged = TRUE) 
      if(is.null(postStats)){ break }
      postStats <- postStats %>% unnest(tags)
      APT <- APT %>% rbind(postStats)
    } else if (type == 'all'){
      postStats <- sproutPostRequest(i, 
                                     dateRange = paste0("created_time.in(", oneYearAgo, "T00:00:00..", currentDate, "T23:59:59)"), 
                                     profileIDs = ids, 
                                     tagged = FALSE) 
      if(is.null(postStats)){ break }
      AP <- AP %>% rbind(postStats)
    }
  }
  
  if(type == 'tagged') return(APT) else return(AP)
  
}

#' get all posts from LinkedIn program accounts 
#' pass account name as an argument to the linkedin paramater
#' bind all
getLIProgramPosts <- function(type){
  LI_CFAN <- cleanPostDF(getPosts('(5381251)', type = type), type = type, linkedin = 'CFAN')
  LI_CCAF <- cleanPostDF(getPosts('(5403265)', type = type), type = type, linkedin = 'CCAF')
  LI_BUILD <- cleanPostDF(getPosts('(5541628)', type = type), type = type, linkedin = 'Buildings')
  LI_TRANSPORT <- cleanPostDF(getPosts('(5635317)', type = type), type = type, linkedin = 'Transportation')
  
  LI_PROGRAM <- LI_CFAN %>% rbind(LI_CCAF) %>% rbind(LI_BUILD) %>% rbind(LI_TRANSPORT)
  return(LI_PROGRAM)
}

#' get post metrics (AVGs) based on all posts made over the last year
getPostAverages <- function(){
  
  posts1YR <- data.frame(created_time = '', post_type = '', text = '', perma_link = '',
                         lifetime.impressions = '', lifetime.post_content_clicks = '',
                         lifetime.engagements = '', lifetime.shares_count = '')[0, ]
  
  #' get all posts from social channels made over the past year
  for(i in 1:300){
    postStats <- sproutPostRequest(i, 
                                   dateRange = paste0("created_time.in(", oneYearAgo, "T00:00:00..", currentDate, "T23:59:59)"),
                                   profileIDs = '(3244287, 2528134, 2528107, 2528104, 3354378, 4145669, 4400432, 4613890, 5083459, 5097954, 5098045, 5251820, 5334423, 5403312, 3246632, 5403593, 5403597)',
                                   tagged = FALSE)
    if(is.null(postStats)){ break }
    
    posts1YR <- posts1YR %>% rbind(postStats)
  }
  
  linkedInAll <- getLIProgramPosts('all')
  
  allPosts1YR <- cleanPostDF(posts1YR, 'all') %>%
    rbind(linkedInAll) %>% 
    #' filter out reposts
    filter(impressions > 0,
           !grepl('ugcPost', perma_link),
           account != '')
  
  posts1YRaverages <- allPosts1YR %>%
    group_by(post_type, account) %>%
    summarize(
      numPosts = n(),
      impressionsMedian = round(median(impressions, na.rm = TRUE), 1),
      impressionsMean = round(mean(impressions, na.rm = TRUE), 1),
      engagementsMedian = round(median(engagements), 1),
      engagementsMean = round(mean(engagements), 1),
      engrtMedian = round(median(engagementRate), 3),
      engrtMean = round(mean(engagementRate), 3)
    ) 
  
  return(posts1YRaverages)
}

#### MONDAY.COM ####

#' Monday.com API call
getMondayCall <- function(x) {
  request <- POST(url = "https://api.monday.com/v2",
                  body = list(query = x),
                  encode = 'json',
                  add_headers(Authorization = mondayToken)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}


