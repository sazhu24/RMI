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

#### API TOKENS & AUTHENTICATION ####

sproutToken <- ## Paste Token Here ##
sproutHeader <- c("Authorization" = sproutToken, "Accept" = "application/json", "Content-Type" = "application/json")

#### FUNCTIONS ####

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
    } else {
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
  LI_CFAN <- cleanPostDF(getPosts('(5381251)', type), type = type, linkedin = 'CFAN')
  LI_CCAF <- cleanPostDF(getPosts('(5403265)', type), type = type, linkedin = 'CCAF')
  LI_BUILD <- cleanPostDF(getPosts('(5541628)', type), type = type, linkedin = 'Buildings')
  LI_TRANSPORT <- cleanPostDF(getPosts('(5635317)', type), type = type, linkedin = 'Transportation')
  
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

#### CODE ####
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

#' set date variables using current date
currentDate <- paste(Sys.Date())
oneYearAgo <- ymd(currentDate) - years(1)

#' get all sprout social tags
metadeta <- getMetadata(url = 'metadata/customer/tags')
tags <- metadeta[["data"]]

#' metadata request
getMetadata <- function(url) {
  request <- GET(url = paste0('https://api.sproutsocial.com/v1/805215/', url),
                 add_headers(sproutHeader)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

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
  
  select(created_time, account, post_type, icon, tag_id, tag_name, impressions, impressionsVmedian, impressionsVmean, engagements, engagementsVmedian, engagementsVmean, 
         engagementRate, engrtVmedian, engrtVmean, postClicks, shares, 
         impressionsMedian, engagementsMedian, engrtMedian, impressionsMean, engagementsMean, engrtMean, brand, program, accountType, post, perma_link, text)

#' write data sets to excel sheet
dataset_names <- list('All Tagged Posts' = taggedPosts, 
                      'Post Metrics (Averages)' = posts1YRaverages)

write.xlsx(dataset_names, file = 'social_media/dataset.xlsx')

