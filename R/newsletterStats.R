library(tidyverse)
library(googlesheets4)
library(rvest)
library(httr)
library(rjson)
library(jsonlite)

### Pardot API Token & Request Headers
token4 <- ###
token5 <- ###
businessID <- ###
header4 <- c("Authorization" = token4, "Pardot-Business-Unit-Id" = businessID)
header5 <- c("Authorization" = token5, "Pardot-Business-Unit-Id" = businessID)

### define functions

## get call
get <- function(url, header) {
  request <- GET(url, add_headers(.headers = header))
  response <- jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8"))
}

## get number of clicks per newsletter
getTotalClicks <- function (df) {
  for(i in 1:(nrow(df) - 1)){
    if(grepl('Fred', df[i + 1, 'name'])|(grepl('^PEM|PR', df[i, 'name']))){
      email_clicks <- GET(paste0("https://pi.pardot.com/api/emailClick/version/4/do/query?format=json&list_email_id=", df[i, 'id']),
                          add_headers(.headers = header4))
      getClicks <- jsonlite::fromJSON(content(email_clicks, as = "text", encoding = "UTF-8"))
      
      df[i, 'totalClicks'] <- getClicks[["result"]][["total_results"]]
    }
  }
  return(df)
}

## get all newsletters since Jan 1, 2023
allNLs <- data.frame(id = '', name = '', subject = '', sentAt = '', createdAt = '')[0,]

for(i in 1:30){
  if(i == 1){
    # write query using next page token to bypass API call limit
    emailLists <- GET('https://pi.pardot.com/api/v5/objects/list-emails?fields=id,name,subject,campaignId,isPaused,isSent,isDeleted,clientType,createdById,updatedById,createdAt,updatedAt,sentAt,operationalEmail,trackerDomainId,emailTemplateId&sentAtAfterOrEqualTo=2023-01-01T15:50:00-04:00',
                      add_headers(.headers = header5))
    res <- jsonlite::fromJSON(content(emailLists, as = "text", encoding = "UTF-8"))
    email_lists <- res[[3]] %>% select(id, name, subject, sentAt, createdAt)
    nextDate <- email_lists[200, 'createdAt']
    allNLs <- allNLs %>% rbind(email_lists)
    
  } else {
    emailLists <- GET(paste0('https://pi.pardot.com/api/v5/objects/list-emails?fields=id,name,subject,campaignId,isPaused,isSent,isDeleted,clientType,createdById,updatedById,createdAt,updatedAt,sentAt,operationalEmail,trackerDomainId,emailTemplateId&sentAtAfter=', nextDate),
                      add_headers(.headers = header5))
    res <- jsonlite::fromJSON(content(emailLists, as = "text", encoding = "UTF-8"))
    email_lists <- res[[3]] %>% select(id, name, subject, sentAt, createdAt) 
    nextDate <- email_lists[200, 'createdAt']
    
    allNLs <- allNLs %>% rbind(email_lists)
    if (nrow(email_lists) < 200) break
  }
}

# filter through list to find correct emails
allSparkNLs <- allNLs %>% 
  filter(grepl('Spark', name) & !grepl('Proof', subject) & !grepl('v2', name)) %>% 
  select(id, name, subject, sentAt) %>% 
  mutate(totalClicks = '')

sparkNLs <- getTotalClicks(allSparkNLs) %>% 
  filter(totalClicks != '' & totalClicks > 0) %>% 
  filter(!grepl('Fred', name)) %>% 
  mutate(name = ifelse(name == 'NL Spark 2023-04-20', 'NL 2023-04-20 Spark', name),
         date = as.Date(str_extract(name, '[0-9]+-[0-9]+-[0-9]+')))
  
allMCNLs <- allNLs %>% 
  filter(!grepl('Proof', subject) & grepl('^PEM', name) & grepl('Transparency Newsletter|Policy Newsletter|Finance Newsletter|Resilience Newsletter|Corporate Newsletter', name)) %>% 
  select(id, name, subject, sentAt) %>% 
  mutate(totalClicks = '') 
  
marketCatalystNLs <- getTotalClicks(allMCNLs) %>% 
  filter(totalClicks != '' & totalClicks > 0) %>% 
  mutate(date = as.Date(paste0(str_extract(name, '[0-9]+-[0-9]+'), '-01'), "%Y-%m-%d")) %>% 
  filter(date >= '2023-06-01') 

## get individual link clicks for each newsletter
getBatchClicks <- function(emailId, df){
  allClicks <- data.frame(id = '', prospect_id = '', url = '', list_email_id = '', email_template_id = '', created_at = '')[0,]
  
  for(i in 1:25){
    if(i == 1){
      # write query using next page token to bypass API call limit
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

# clean links and grab utm parameters
sparkClicks <- function (emailId, newsletter) {
  
  batchClicks <- getBatchClicks(emailId, newsletter)
  groupedClicks <- batchClicks %>% 
    distinct(prospect_id, url, .keep_all = TRUE) %>% 
    mutate(url = ifelse(grepl('term=question', url), paste('Question -', sub('\\?(.*)', '', url)), url),
           url = ifelse(grepl('term=chart', url), paste('Chart -', sub('\\?(.*)', '', url)), url),
           url = ifelse(grepl('https://rmi.org/events/', url), paste('Events -', sub('\\?(.*)', '', url)), url),
           plain_text = ifelse(grepl('plain-text', url), TRUE, FALSE),
           url = gsub('\\&utm_content=spark-b|\\&utm_content=spark-a|\\&utm_term=plain-text|-plain-text|\\&utm_term=text', '', url)) %>% 
    mutate(url = ifelse(grepl('https://give.rmi.org/give/286001/#!/donation/checkout', url), 'Donate', url),
           url = ifelse(grepl('https://rmi.org/subscribe/', url), 'Subscribe', url),
           url = ifelse(grepl('https://rmi.org/stories/', url), 'Stories', url),
           url = ifelse(grepl('https://rmi.org/research/', url), 'Research', url),
           url = ifelse(grepl('https://rmi.org/our-work/', url), 'Our Work', url),
           url = sub('\\?(.*)', '', url)
    ) 
  
  plainText <- groupedClicks %>% 
    filter(plain_text == TRUE) %>% 
    group_by(url) %>% 
    summarise(clicks = n()) %>% 
    arrange(-clicks)
  
  allLinkClicks <- groupedClicks %>% 
    group_by(url) %>% 
    summarise(clicks = n()) %>% 
    arrange(-clicks) 
  
  plainTextPCT <- sum(plainText$clicks, na.rm = FALSE)/sum(allLinkClicks$clicks, na.rm = FALSE) #0.5
  
  allLinkClicks <- allLinkClicks %>% 
    mutate(email = paste(newsletter[emailId, 'name']),
           plainTextPCT = round(plainTextPCT, 3))
  
  return(allLinkClicks)
}

# get link clicks for all emails, pivot wider and bind into one data frame
getLinkClicks <- function(newsletter) {
  allCleaned <- data.frame(email = '', url_1 = '', clicks_1 = '', url_2 = '', clicks_2 = '', url_3 = '', clicks_3 = '', 
                           url_chart = '', clicks_chart = '', url_question = '', clicks_question = '', url_events = '', clicks_events = '',
                           plainTextPCT = '')[0,]
  
  for(i in 1:nrow(newsletter)){
  
    if(deparse(substitute(newsletter)) == 'sparkNLs'){
      NL <- sparkClicks(i, newsletter) %>% 
        mutate(story = '') %>% 
        filter(grepl("https://rmi.org", url)) %>% 
        arrange(-clicks) 
      
      NL[, 'story'] <- paste0(rownames(NL))
      chart <- NL %>% filter(grepl('Chart', url)) 
      question <- NL %>% filter(grepl('Question', url)) 
      events <- NL %>% filter(grepl('Events', url))
      
      if(nrow(chart) == 0){ chart <- NL[1,] %>% mutate(url = 'Chart - NA', clicks = 0, story = 0) }
      if(nrow(question) == 0){ question <- NL[1,] %>% mutate(url = 'Question - NA', clicks = 0, story = 0) }
      if(nrow(events) == 0){ events <- NL[1,] %>% mutate(url = 'Events - NA', clicks = 0, story = 0) }
      
      top_wider <- NL %>% 
        filter(grepl("^https://rmi.org", url)) 
      
      top_wider <- top_wider[c(1:3),] %>% 
        rbind(chart) %>% rbind(question) %>% rbind(events)
      
      top_wider[, 'story'] <- paste0(rownames(top_wider))
      
      for(j in 1:nrow(top_wider)){
        if(grepl('Question', top_wider[j, 'url'])) { top_wider[j, 'story'] <- paste0('question') }  
        if(grepl('Chart', top_wider[j, 'url'])) { top_wider[j, 'story'] <- paste0('chart') }  
        if(grepl('Events', top_wider[j, 'url'])) { top_wider[j, 'story'] <- paste0('events') }  
      }
      
      top_wider <- top_wider %>% 
        pivot_wider(names_from = story, values_from = c(url, clicks), names_vary = "slowest") 
      
      print(paste0(i, ': ', top_wider$email))
      allCleaned <- allCleaned %>% rbind(top_wider)
      
    } else if(deparse(substitute(newsletter)) == 'marketCatalystNLs'){
      NL <- sparkClicks(i, newsletter) %>% 
        mutate(story = '') %>% 
        filter(grepl("https://rmi.org", url)) %>% 
        arrange(-clicks) 
      
      NL[, 'story'] <- paste0(rownames(NL))
      
      top_wider <- NL %>% 
        filter(grepl("^https://rmi.org", url)) 
      
      top_wider <- top_wider[c(1:3),] 
      
      top_wider[, 'story'] <- paste0(rownames(top_wider))
      
      top_wider <- top_wider %>% 
        pivot_wider(names_from = story, values_from = c(url, clicks), names_vary = "slowest") 
      
      print(paste0(i, ': ', top_wider$email))
      allCleaned <- allCleaned %>% rbind(top_wider)
    }
  }
  return(allCleaned)
}

allSpark <- getLinkClicks(newsletter = sparkNLs) 
allMC <- getLinkClicks(newsletter = marketCatalystNLs) 

### get email stats

newDF <- function(template) {
  df <- data.frame()
  for(i in 1:length(names(template))){
    colname <- names(template)[i]
    df <- df %>% add_column(x = "") 
    colnames(df)[i] = colname
  }
  return(df)
}

##
emailStats <- GET(paste0('https://pi.pardot.com/api/email/version/4/do/stats/id/1397547468?format=json'),
                  add_headers(.headers = header4))
getStats <- fromJSON(content(emailStats, as = "text", encoding = "UTF-8"))
stats <- as.data.frame(getStats[["stats"]]) 

# get email stats for all emails

getEmailStats <- function(newsletter){
  allEmailStats <- newDF(stats)
  
  for(i in 1:nrow(newsletter)){
    url <- paste0('https://pi.pardot.com/api/email/version/4/do/stats/id/', newsletter[i, 'id'], '?format=json')
    getStats <- get(url,  header4)
    stats <- as.data.frame(getStats[["stats"]]) %>% 
      mutate(id = paste(newsletter[i, 'id']),
             name = paste(newsletter[i, 'name']),
             subject = paste(newsletter[i, 'subject']))
    allEmailStats <- allEmailStats %>% rbind(stats)
  }
  
  allEmailStats <- allEmailStats %>% 
    select(id, name, subject, sent, delivered, delivered_rate = delivery_rate, unique_opens, open_rate = opens_rate, total_clicks, 
           unique_clicks, unique_CTR = unique_click_through_rate, click_open_ratio, opt_outs, opt_out_rate) %>% 
    mutate(across(sent:opt_out_rate, ~ as.numeric(gsub('%', '', .x)))) %>% 
    mutate_at(c("delivered_rate", "open_rate", "unique_CTR", "click_open_ratio"), ~ (.x/100)) %>% 
    mutate(name = ifelse(name == 'NL Spark 2023-04-20', 'NL 2023-04-20 Spark', name))
  
  return(allEmailStats)
}

allSparkEmailStats <- getEmailStats(sparkNLs)
allMCEmailStats <- getEmailStats(marketCatalystNLs)

# clean output

getSparkStats <- function(emailStatsDF = allSparkEmailStats, emailClicksDF = allSpark){
 
  CTR_mean <- (sum(emailStatsDF$unique_CTR)/length(emailStatsDF$unique_CTR))
  openRT_mean <- (sum(emailStatsDF$open_rate)/length(emailStatsDF$open_rate))
  
  allEmailStats2 <- emailStatsDF %>% 
    mutate(UCTRvsAvg = 10*(unique_CTR - CTR_mean),
           ORvsAvg = (open_rate - openRT_mean)) %>% 
    mutate_at(c("open_rate", "unique_CTR", "click_open_ratio", 'UCTRvsAvg', 'ORvsAvg'), ~ round(.x, 3)) %>% 
    relocate(UCTRvsAvg, .after = unique_CTR) %>% 
    relocate(ORvsAvg, .after = open_rate) %>% 
    left_join(emailClicksDF, by = c('name' = 'email')) %>% 
    mutate(COR_S1 = clicks_1 / unique_opens / 100,
           COR_S2 = clicks_2 / unique_opens / 100,
           COR_S3 = clicks_3 / unique_opens / 100,
           COR_chart = clicks_chart / unique_opens / 100,
           COR_question = clicks_question / unique_opens / 100,
           COR_events = clicks_events / unique_opens / 100,
           rows = '') %>% 
    mutate_at(c('url_1', 'url_2', 'url_3', 'url_chart', 'url_question', 'url_events'), ~ sub('\\?(.*)', '', .x)) %>% 
    mutate(date = as.Date(str_extract(name, '[0-9]+-[0-9]+-[0-9]+')))
  
  allEmailStats3 <- allEmailStats2[rev(order(allEmailStats2$date)),]
  rownames(allEmailStats3) <- NULL
  allEmailStats3[, 'rows'] <- as.numeric(rownames(allEmailStats3)) + 1
  
  # add formula to locate titles for URLs using google sheets
  allEmailStats4 <- allEmailStats3 %>% 
    mutate(title_1 = gs4_formula(paste0('=HYPERLINK(S', rows, ', IMPORTXML(S', rows, ', "//head//title") )')),
           title_2 = gs4_formula(paste0('=HYPERLINK(W', rows, ', IMPORTXML(W', rows, ', "//head//title") )')),
           title_3 = gs4_formula(paste0('=HYPERLINK(AA', rows, ', IMPORTXML(AA', rows, ', "//head//title") )'))) %>% 
    mutate_at(c('COR_S1', 'COR_S2', 'COR_S3', 'COR_chart', 'COR_events'), ~ round(.x * 100, 3)) %>% 
    mutate_at(c('COR_question'), ~ round(.x * 100, 4)) %>% 
    mutate_at(c('url_chart', 'url_question', 'url_events'), ~ ifelse(grepl('NA', .x), NA, .x)) %>% 
    mutate_at(c('clicks_chart', 'COR_chart', 'clicks_question', 'COR_question', 'clicks_events', 'COR_events'), ~ ifelse(.x == 0.000, NA, .x)) %>% 
    mutate_at(c('url_chart', 'url_question', 'url_events'), ~ sub('(.*) - ', '', as.character(.x))) %>% 
    mutate(COR_chart = ifelse(is.na(clicks_chart), NA, COR_chart),
           COR_question = ifelse(is.na(clicks_question), NA, COR_question),
           COR_events = ifelse(is.na(clicks_events), NA, COR_events)) %>% 
    select(id, name, date, subject_line = subject, sent, delivered, delivered_rate, unique_opens, open_rate, ORvsAvg, total_clicks, unique_clicks, unique_CTR, UCTRvsAvg, click_open_ratio, opt_outs, opt_out_rate, plaintext_rate = plainTextPCT, 
           url_1, title_1, clicks_1, COR_S1, url_2, title_2, clicks_2, COR_S2, url_3, title_3, clicks_3, COR_S3,
           url_chart, clicks_chart, COR_chart, url_question, clicks_question, COR_question, url_events, clicks_events, COR_events) 
  
  return(allEmailStats4)
}

allSparkStats <- getSparkStats()

# push data
range_flood(ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Spark Stats', range = "A2:AM100", reformat = FALSE)
write_sheet(allSparkStats, ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Spark Stats')
Sys.sleep(10)
range_flood(ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Spark Stats (Unformatted)', range = "A2:AM100")
write_sheet(allSparkStats, ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Spark Stats (Unformatted)')

###

getMCStats <- function(emailStatsDF = allMCEmailStats, emailClicksDF = allMC){
  
  allEmailStats2 <- emailStatsDF %>% 
    mutate_at(c("open_rate", "unique_CTR", "click_open_ratio"), ~ round(.x, 3)) %>% 
    left_join(emailClicksDF, by = c('name' = 'email')) %>% 
    mutate(COR_S1 = clicks_1 / unique_opens / 100,
           COR_S2 = clicks_2 / unique_opens / 100,
           COR_S3 = clicks_3 / unique_opens / 100,
           rows = '') %>% 
    mutate_at(c('url_1', 'url_2', 'url_3'), ~ sub('\\?(.*)', '', .x)) %>% 
    mutate(name = ifelse(grepl('PEM 2023-07 Finance Newsletter', name), 'PEM 2023-07-12 Finance Newsletter', name),
           date = as.Date(stringr::str_extract(name, '[0-9]+-[0-9]+-[0-9]+')))
  
  allEmailStats2 <- allEmailStats2[rev(order(allEmailStats2$date)),]
  rownames(allEmailStats2) <- NULL
  allEmailStats2[, 'rows'] <- as.numeric(rownames(allEmailStats2)) + 1
  
  # add formula to locate titles for URLs using google sheets
  allEmailStats3 <- allEmailStats2 %>% 
    mutate(title_1 = gs4_formula(paste0('=HYPERLINK(Q', rows, ', IMPORTXML(Q', rows, ', "//head//title") )')),
           title_2 = gs4_formula(paste0('=HYPERLINK(U', rows, ', IMPORTXML(U', rows, ', "//head//title") )')),
           title_3 = gs4_formula(paste0('=HYPERLINK(Y', rows, ', IMPORTXML(Y', rows, ', "//head//title") )'))) %>% 
    mutate_at(c('COR_S1', 'COR_S2', 'COR_S3'), ~ round(.x * 100, 3)) %>% 
    select(id, name, date, subject_line = subject, sent, delivered, delivered_rate, unique_opens, open_rate, total_clicks, unique_clicks, 
           unique_CTR, click_open_ratio, opt_outs, opt_out_rate, plaintext_rate = plainTextPCT, 
           url_1, title_1, clicks_1, COR_S1, url_2, title_2, clicks_2, COR_S2, url_3, title_3, clicks_3, COR_S3) 
  
  return(allEmailStats3)
}

allMCStats <- getMCStats()

# push data
range_flood(ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Market Catalyst Stats', range = "A2:AM100", reformat = FALSE)
write_sheet(allMCStats, ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Market Catalyst Stats')
Sys.sleep(10)
range_flood(ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Market Catalyst Stats (Unformatted)', range = "A2:AM100", reformat = FALSE)
write_sheet(allMCStats, ss = 'https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=533592380', sheet = 'All Market Catalyst Stats (Unformatted)')
