library(tidyverse)
library(xml2)
library(httr)
library(rjson)
library(jsonlite)
library(lubridate)

## write to this sheet
ss <- 'https://docs.google.com/spreadsheets/d/1_wUKRziRhF90ZfUemHNt3DaOjU2fQlTIPO8rNlA9doY/edit#gid=1033361432' ## OCI

### Monday.com API Token
mondayToken <- 'eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjI2NDQzMjQyNiwiYWFpIjoxMSwidWlkIjozNTY3MTYyNSwiaWFkIjoiMjAyMy0wNi0yMlQxNjo0NTozOS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MjY4NTM2NiwicmduIjoidXNlMSJ9.KsZ9DFwEXeUuy23jRlCGauiyopcUFTHF6WciunTLFLM'

getMondayCall <- function(x) {
  request <- POST(url = "https://api.monday.com/v2",
                  body = list(query = x),
                  encode = 'json',
                  add_headers(Authorization = mondayToken)
  )
  return(jsonlite::fromJSON(content(request, as = "text", encoding = "UTF-8")))
}

# Get Active Projects Board
query <- "query { boards (ids: 2208962537) { items { id name column_values{ id value text } } } } "
res <- getMondayCall(query)
activeProjects <- as.data.frame(res[["data"]][["boards"]][["items"]][[1]])

# Iterate through APB to find projects with "Metrics Dashboard" in the promotion tactics column
campaigns <- data.frame(id = '', row = '', name = '')[0,]
for(i in 1:nrow(activeProjects)){
  #i <- 1
  board <- activeProjects[[3]][[i]]
  if(grepl('Metrics Dashboard', paste(board[11, 'text']))){
    campaigns <- campaigns %>% 
      rbind(c(paste(activeProjects[i, 'id']), i, c(paste(activeProjects[i, 'name'])))) 
  }
}
names(campaigns) <- c('id', 'row', 'name')

# Filter to identify campaign of interest
targetCampaign <- campaigns %>% 
  filter(grepl('CIP: Coal v Gas campaign', name))

# Get metrics, audiences, and ID
campaignRow <- as.numeric(targetCampaign[1, 'row'])
campaignBoard <- activeProjects[[3]][[campaignRow]]
campaignDF <- data.frame(campaignID = campaignBoard[16, 'text'], 
                         campaignAudiences = campaignBoard[15, 'text'], 
                         campaignMetrics = campaignBoard[11, 'text'])

# Push data
write_sheet(campaignDF, ss = ss, sheet = 'Campaign Overview')
