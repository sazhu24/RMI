
#############
# FUNCTIONS #
#############

#### GOOGLE ANALYTICS

## get page data
getPageData <- function(df){
  
  for(i in 1:nrow(df)){
    
    url <- as.character(df[i, 'pageURL'])
    
    ## get page titles
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
    
    ## get page types from metadata
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
      df[i, 'metadata'] <- NA
      df[i, 'pageType'] <- 'New Website'
    }
    
  }
  
  df <- df %>% 
    mutate(pageType = ifelse(grepl('article', tolower(metadata)), 'Article',
                             ifelse(grepl('report', tolower(metadata)), 'Report', pageType)),
           icon = ifelse(grepl('article', tolower(metadata)), 4,
                         ifelse(grepl('report', tolower(metadata)), 1, 5))) %>% 
    distinct(pageTitle, .keep_all = TRUE)
  
}

## get web traffic and key metrics for all pages
getPageMetrics <- function(propertyID, pages){
  campaignPages <- ga_data(
    propertyID,
    metrics = c('screenPageViews', "totalUsers", "userEngagementDuration", 'sessions'),
    dimensions = c("pageTitle"),
    date_range = dateRangeGA,
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

getTrafficGeography <- function(propertyID, pages, site = 'rmi.org'){
  
  ## get page views by region
  trafficByRegion <- ga_data(
    propertyID,
    metrics = c('screenPageViews'),
    dimensions = c("pageTitle", 'region', 'country'),
    date_range = dateRangeGA,
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
    date_range = dateRangeGA,
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
      date_range = dateRangeGA,
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

#### PARDOT / EMAILS

### Email Stats

getAllEmailStats <- function(){
  
  # find link on Email Stats spreadsheet
  emailStatsSpark <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Spark Stats (Unformatted)') %>% 
    mutate(date = as.Date(date))
  
  # find link on Email Stats spreadsheet
  emailStatsPEM <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=1938257643', sheet = 'All Market Catalyst Stats (Unformatted)') 
  
  allEmailStats <- emailStatsSpark %>% 
    plyr::rbind.fill(emailStatsPEM) %>% 
    mutate(name = ifelse(grepl('Spark', name), paste0(date, ': Spark'), paste0(date, ':', sub('(.*)-[0-9]{2}', '', name))),
           id = as.numeric(id))
  
  return(allEmailStats)
}


getCampaignEmails <- function(pageURLs){
  
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
           story_title = gsub(' - RMI', '', story_title),
           dashboardCampaign = campaignID)
  
  # add icon 
  allStoryStats <- allStoryStats[rev(order(allStoryStats$date)),]
  rownames(allStoryStats) <- NULL
  allStoryStats[, 'icon'] <- as.numeric(rownames(allStoryStats))
  
  return(allStoryStats)
}

### Email Clicks

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


#### SALESFORCE

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

### REPORTS AND EVENTS

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

### get donations
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


#### SPROUT SOCIAL

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
cleanPostDF <- function(df, type, linkedin = 'FALSE'){
  
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
    postStats <- sproutPostRequest(i, paste0("created_time.in(2023-04-01T00:00:00..", currentDate, "T23:59:59)"), profileIDs = ids) 
    if(is.null(postStats)){ break }
    
    # data frame - posts with all tags
    postStats <- postStats %>% unnest(tags)
    APT <- APT %>% rbind(postStats)
    
  }
  
  return(APT)
  
}

## get and bind all posts from program accounts
getLIProgramPosts <- function(type){
  LICFAN <- cleanPostDF(getPosts('(5381251)', type), type = type, linkedin = 'CFAN')
  LICCAF <- cleanPostDF(getPosts('(5403265)', type), type = type, linkedin = 'CCAF')
  LIBUILD <- cleanPostDF(getPosts('(5541628)', type), type = type, linkedin = 'Buildings')
  LITRANSPORT <- cleanPostDF(getPosts('(5635317)', type), type = type, linkedin = 'Transportation')
  
  LI <- LICFAN %>% rbind(LICCAF) %>% rbind(LIBUILD) %>% rbind(LITRANSPORT)
  return(LI)
}


#### MONDAY.COM

getMondayCall <- function(x) {
  request <- POST(url = "https://api.monday.com/v2",
                  body = list(query = x),
                  encode = 'json',
                  add_headers(Authorization = mondayToken)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}


