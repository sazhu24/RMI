library(tidyverse)
library(openxlsx)
library(googledrive)
library(fuzzyjoin)
library(tidytext)
library(googlesheets4)
library(salesforcer)
library(rvest)
library(xml2)
library(httr)

### define campaign variables
campaigns_reports <- c('7016f0000023VTHAA2', '7016f0000013txRAAQ', '7016f0000023TgYAAU', '7016f0000013vm9AAA') 

campaigns_events <- c('7016f0000023VTgAAM') # WBN: OCI +

campaignEmailIDs <- c(1389821559, 1327603672)

methanelinks <- c('https://rmi.org/oci-update-tackling-methane-in-the-oil-and-gas-sector/',
                  'https://rmi.org/clean-energy-101-methane-detecting-satellites/',
                  'https://rmi.org/waste-methane-101-driving-emissions-reductions-from-landfills/')

### Pardot API Request Headers
token4 <- 'Bearer 00DU0000000HJDy!ARAAQMY_LXHSWVXEkOUPvGIWM68.6Qi6VY70ptLJMZuR2Q6bdQNQuY0Sywp9xiMeUl2yYMg5RXgURwPJZS3q6SbOEKtd0oJM'
token5 <- "Bearer 00DU0000000HJDy!ARAAQAHEYGZ4Fwe9hzW2Tqakqz2k2TTnj20keB9t6UnB8L4fkcvewtwmCsh0A5VHN_A9BYwi0tHWXKlAGio7a577wpC..qbJ"
header4 <- c("Authorization" = token4, "Pardot-Business-Unit-Id" = "0Uv0B000000XZDkSAO")
header5 <- c("Authorization" = token5, "Pardot-Business-Unit-Id" = "0Uv0B000000XZDkSAO")

## write to this sheet
ss <- 'https://docs.google.com/spreadsheets/d/1_wUKRziRhF90ZfUemHNt3DaOjU2fQlTIPO8rNlA9doY/edit#gid=1033361432' ## OCI

### FUNCTIONS

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
  
  my_soql <- sprintf("SELECT Id, Name, Account_Name_Text__c, Title, Email,
                             pi__url__c, pi__score__c, pi__last_activity__c,
                             MailingCountry, MailingState, MailingCity, Account_Record_Type__c
                           
                    FROM Contact
                    WHERE pi__last_activity__c != null 
                    ORDER BY pi__last_activity__c DESC")
  
  contactsQuery <- sf_query(my_soql, 'Contact', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Account_Name_Text__c, Title, Email, 
           Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c,
           Country = MailingCountry, State = MailingState, City = MailingCity) %>% 
    mutate(RecordType = 'Contact')
  
  my_soql <- sprintf("SELECT Id,  Name, Company, Title, Industry, Email,
                           pi__url__c, pi__score__c, pi__last_activity__c,
                           Country, State, City

                    FROM Lead
                    WHERE pi__last_activity__c != null 
                    ORDER BY pi__last_activity__c DESC")
  
  leadsQuery <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Title, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c,
           Country, State, City) %>% 
    mutate(RecordType = 'Lead')
  
  prospects <- contactsQuery %>% rbind(leadsQuery) ## 209,614
  
  # identify and remove duplicates
  dup <- prospects[duplicated(prospects[,c("Pardot_URL")]) | duplicated(prospects[,c("Pardot_URL")], fromLast = TRUE), ] 
  
  dup <- dup %>% 
    mutate(Account = ifelse(grepl('unknown|not provided', tolower(Account))|Account == '', NA, Account)) %>% 
    filter(is.na(Account)|Account != '') %>% 
    filter(RecordType == 'Lead') %>% 
    distinct(Pardot_URL, .keep_all = TRUE)
  
  prospects_bad <- prospects %>% filter(!is.na(Email)) %>% anti_join(dup)
  unique_prospects <- prospects_bad[!duplicated(prospects_bad[, 'Pardot_URL']), ]
  
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
  all_accounts <- sf_query(my_soql, "Account", api_type = "Bulk 1.0")
  return(all_accounts)
}

##### REPORTS AND EVENTS

# get list of campaigns
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
campaign_list <- sf_query(my_soql) %>% 
  select(Id, Name, Type, CreatedDate, Members = Members_in_Campaign__c)

# get list of all reports
reports <- campaign_list %>% 
  filter(Type == 'Report') %>% 
  filter(grepl('^RP', Name)) %>% 
  filter(Members > 10)

campaignsReports <- reports$Id

# get list of all events
events <- campaign_list %>% 
  filter(grepl('Event|Training|Workshop', Type) & Type != 'Development Event')

#campaigns_events <- events$Id

# get campaign members and join to contact/lead/account info
getCampaignMembers <- function(campaignIdList, campaignType) {
  #campaignIdList <- campaigns_events
  # get campaign members
  if(length(campaignIdList) == 0){
    return()
  } else{
    
  }
  my_soql <- sprintf("SELECT CampaignId,
                             Name,
                             Status,
                             HasResponded,
                             ContactId,
                             LeadId,
                             AccountId,
                             CreatedDate

                      FROM CampaignMember
                      WHERE CampaignId in ('%s')",
                     paste0(campaignIdList, collapse = "','"))
  campaign_members <- sf_query(my_soql, 'CampaignMember', api_type = "Bulk 1.0") %>% 
    left_join(select(campaign_list, c(CampaignId = Id, CampaignName = Name)), by = 'CampaignId') %>% 
    filter(!(is.na(as.character(ContactId)) & is.na(LeadId)))
  
  campaignContacts <- campaign_members %>% filter(!is.na(ContactId)) %>% distinct(ContactId, .keep_all = TRUE)
  campaignLeads <- campaign_members %>% filter(!is.na(LeadId)) %>% distinct(LeadId, .keep_all = TRUE)
  
  
  # contacts in campaign
  my_soql <- sprintf("SELECT Id, Name, Account_Name_Text__c, AccountId, Title, Email,
                             pi__url__c, pi__score__c, pi__last_activity__c,
                             Giving_Circle__c, Top_Philanthropic_and_Wealth_Lists__c,
                             Account_Largest_Gift__c, Number_of_Days_since_Last_Gift_or_SC__c,
                             MailingCountry, MailingState, MailingCity
                             
                      FROM Contact
                      WHERE Id in ('%s')
                      ORDER BY pi__last_activity__c DESC NULLS LAST", 
                      paste0(campaignContacts$ContactId, collapse = "','"))
  
  campaignContactsQuery <- sf_query(my_soql, 'Contact', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Account_Name_Text__c, Title, Email, 
           Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c,
           Country = MailingCountry, State = MailingState, City = MailingCity, 
           Giving_Circle = Giving_Circle__c, Top_Philanthropic = Top_Philanthropic_and_Wealth_Lists__c, Last_Gift = Number_of_Days_since_Last_Gift_or_SC__c,
           AccountId) %>% 
    mutate(RecordType = 'Contact') %>% 
    filter(!is.na(AccountId))
  
  # leads in campaign
  my_soql <- sprintf("SELECT Id, Name, Company, Title, Email,
                           pi__url__c, pi__score__c, pi__last_activity__c,
                           Country, State, City

                    FROM Lead
                    WHERE Id in ('%s')
                    ORDER BY pi__last_activity__c DESC NULLS LAST", 
                     paste0(campaignLeads$LeadId, collapse = "','"))
  
  campaignLeadsQuery <- sf_query(my_soql, 'Lead', api_type = "Bulk 1.0") %>% 
    select(Id, Name, Account = Company, Title, Email, Pardot_URL = pi__url__c, Pardot_Score = pi__score__c, Pardot_LastActivity = pi__last_activity__c, Country, State, City) %>% 
    mutate(Giving_Circle = '',
           Top_Philanthropic = '',
           Last_Gift = '',
           RecordType = 'Lead')
  
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
           TotalGiving = Total_Opportunity_Payments__c, NumGifts = of_Gifts__c, NumOpenOpps = Number_of_Open_Opportunities__c,
           DB_IndustryCategory = D_B_Major_Industry_Category_Name__c, NAICS1 = D_B_NAICS_Description_1__c, SIC1 = D_B_SIC4_Code_1_Description__c)
  
  campaignContactsQuery <- campaignContactsQuery %>% 
    left_join(select(campaignContactAccounts, -Account), by = c('AccountId'))
  
  campaignLeadsQuery <- campaignLeadsQuery %>% 
    left_join(campaignContactAccounts, by = c('Account')) %>% 
    relocate(AccountId, .after = 'Last_Gift')
  
  contactsLeads <- campaignContactsQuery %>%
    rbind(campaignLeadsQuery) %>% 
    mutate(Domain = sub("(.*)\\@", "", Email)) %>% 
    relocate(Domain, .after = "Email") %>% 
    relocate(RecordType, .after = Id) 
  
  CampaignMembers <- campaign_members %>% 
    mutate(Id = ifelse(is.na(ContactId), LeadId, ContactId)) %>% 
    select(CampaignId, CampaignName, Name, Id, Status, CreatedDate) %>% 
    left_join(contactsLeads, by = c('Id', 'Name'))
  
  if(campaignType == 'Event'){
    CampaignMembers <- CampaignMembers %>% mutate(EngagementType = 'Event')
  } else if (campaignType == 'Report'){
    CampaignMembers <- CampaignMembers %>% mutate(EngagementType = 'Report Download')
  }
  
  return(CampaignMembers)
  
}

print('getting event campaigns')
campaignMembersEvents <- getCampaignMembers(campaigns_events, 'Event')

print('getting report campaigns')
campaignMembersReports <- getCampaignMembers(campaigns_reports, 'Report') 


##### GET EMAIL CLICKS
print('getting email clicks')

## get all prospects
prospects <- getProspects() %>% 
   mutate(Domain = sub("(.*)\\@", "", Email))

## get all accounts
all_accounts <- getAllAccounts() %>% 
  select(AccountId = Id, Account = Name, AccountType = Type, Industry, AccountDomain = Email_Domain__c,
         Website, DB_Website = D_B_Web_Address__c, DB_IndustryCategory = D_B_Major_Industry_Category_Name__c, NAICS1 = D_B_NAICS_Description_1__c, 
         NAICS2 = D_B_NAICS_Description_2__c, SIC1 = D_B_SIC4_Code_1_Description__c, SIC2 = D_B_SIC4_Code_2_Description__c,
         TopAccount = Top_Account__c, TotalGiving = Total_Opportunity_Payments__c, NumGifts = of_Gifts__c, NumOpenOpps = Number_of_Open_Opportunities__c)

## get domain info for gov accounts
domainKey <- read_sheet('https://docs.google.com/spreadsheets/d/17xDNEjntshgoCfRS4SFkuLKTrHJBxyHmFWzUNS1A-QE/edit#gid=10923162') %>% 
  mutate(name = ifelse(level == 'LOCAL', sub(',(.*)', '', paste0('City of ', name)), name))

## get audience domains and accounts
audienceDomainsAccounts <- read.xlsx('/Users/sara/Desktop/R/RMI/Audience_Insights/campaignDashboard/audienceDomainsAccounts.xlsx')
audienceDomainsAccounts[1, 'financialAccounts'] <- paste('The World Bank Group|Bank of America|Citi|Wells Fargo|BlackRock|International Finance Corporation (IFC)|ING Group|CIBC|Barclays|Goldman Sachs|Morgan Stanley|Royal Bank of Canada|TD Bank|Societe Generale|Wells Fargo Foundation|HSBC|Standard Chartered Bank|UBS Group AG|Macquarie Group|UniCredit|BNP Paribas|Quadrature Climate Foundation|Climate Bonds|Deutsche Bank|Energy Impact Partners|Inter-American Development Bank|JPMorgan Chase & Co.|Credit Agricole Group|Climate Bonds Initiative|CoBank|Marathon Capital|Mastercard|Plug In America|SkyNRG|Swiss Re Group|8 Rivers Capital|ABN AMRO|American Express|Arabella Advisors|Banco Bilbao Vizcaya Argentaria|Banco Itaú Unibanco S.A.|Banco Santander, S.A.|CLP Holdings Ltd.|CrossBoundary Energy|Invesco Real Estate|MAP Energy, LLC|Perella Weinberg Partners|Renewal Funds|Silicon Valley Bank|State Street Corporation|Sunpin Solar|U.S. Department of Housing and Urban Development|Amalgamated Bank|Americans For Financial Reform|CT GreenBank|Circle Economy|Clarion Partners|Dutch Postcode Lottery|Eindhoven University of Technology|Element 8|Fitch Ratings|Generation Foundation|Global Cleantech Capital|Guzman Energy|HEET (Home Energy Efficiency Team)|Hobbs and Towne, Inc.|Institute for Energy Economics and Financial Analysis (IEEFA)|KfW Group|Kleiner Perkins|Ministry of Water, Irrigation, and Energy|NGP Energy Technology Partners|New Island Capital|NuCapital Inc|Oaktree Capital Management, LLC|PA Consulting|Quadrature Capital|RBC Capital Markets|Rabobank|Sarasin & Partners|Scotiabank|Seeder|Syntrus Achmea Real Estate|The Community Preservation Corporation (CPC)')
audienceDomainsAccounts[1, 'financialDomains'] <- paste('worldbank.org|bofa.com|citi.com|wellsfargo.com|blackrock.com|ifc.org|ing.com|cibc.com|barclays.com|gs.com|morganstanley.com|rbc.com|td.com|sgcib.com|hsbc.com|sc.com|ubs.com|macquarie.com|unicredit.eu|bnpparibas.com|climatebonds.net|db.com|energyimpactpartners.com|iadb.org|jpmchase.com|ca-cib.com|selcnc.org|climatebonds.net|cobank.com|marathon-cap.com|mastercard.com|skynrg.com|swissre.com|8rivers.com|nl.abnamro.com|aexp.com|arabellaadvisors.com|bbva.com|itaubba.com|gruposantander.com|clp.com.hk|crossboundary.com|greenmountainpower.com|invesco.com|maproyalty.com|pwpartners.com|renewalfunds.com|svb.com|ssga.com|amalgamatedbank.com|ourfinancialsecurity.org|ctgreenbank.com|circle-economy.com|clarionpartners.com|generationim.com|gccfund.net|ieefa.org|kfw.de|kpcb.com|ngpetp.com|nucapital.nl|oaktreecapital.com|quadraturecapital.com|rbccm.com|rabobank.nl|sarasin.co.uk|scotiabank.com|communityp.com')
audienceDomainsAccounts[1, 'multilateralAccounts'] <- paste('World Bank|United Nations Foundation|United Nations Development Programme|World Business Council for Sustainable Development|Global Green Growth Institute|European Investment Bank|African Development Bank|Development Bank of Southern Africa|United Nations|Africa Finance Corporation')
audienceDomainsAccounts[1, 'multilateralDomains'] <- paste('worldbank.org|un.org|undp.org|afdb.org|dbsa.org|unido.org')
audienceDomainsAccounts[1, 'NGOAccounts'] <- paste('World Resources Institute|Sierra Club|The World Bank Group|Natural Resources Defense Council|Environmental Defense Fund|International Finance Corporation (IFC)|International Energy Agency|The Climate Group|United Nations Foundation|EarthJustice|C40 Cities Climate Leadership Group - Clinton Climate Initiative|Ceres|American Council for an Energy-Efficient Economy|Energy Foundation|ClimateWorks Foundation|World Economic Forum|The Regulatory Assistance Project (RAP)|Vermont Energy Investment Corporation (VEIC)|The Nature Conservancy|European Climate Foundation|Greenpeace|Smart Electric Power Alliance|E3G|The International Council on Clean Transportation|Vote Solar Initiative|World Business Council for Sustainable Development|CALSTART|Slipstream Inc.|Global Green Growth Institute|U.S. Green Building Council|Union of Concerned Scientists (UCS)|International Renewable Energy Agency|Carbon Tracker|The Pembina Institute|International Council of Local Environmental Initiatives|Inter-American Development Bank|UN SEforAll|California Center for Sustainable Energy|Organisation For Economic Co-operation And Development|Western Resource Advocates|Rewiring America|Climate Analytics|Edison Electric Institute|RTI International|Energy Innovation Policy and Technology LLC|Elevate Energy|Fresh Energy|European Investment Bank|New Buildings Institute|BSR|ITRI|The Rockefeller Foundation|Bloomberg Philanthropies|Clean Energy Buyers Alliance|Urban Land Institute|GRID Alternatives|Institute for Market Transformation|Northeast Energy Efficiency Partnerships (NEEP)|Northwest Energy Efficiency Alliance|ClimateWorks Australia|Climate Nexus|National Association of Regulatory Utility Commissioners (NARUC)|Center for Energy and Environment|Institute for Energy Economics and Financial Analysis (IEEFA)|Tacoma Public Utilities|Solar Energy Industries Association|Blue Green Alliance|World Wildlife Fund|WattTime|Solar United Neighbors|African Development Bank|Colorado Springs Utilities|Alliance to Save Energy|Breakthrough Energy|New Climate Institute|Nationally Determined Contributions (NDC) Partnership|Center for American Progress|Net Zero Asset Owners Alliance|Green Climate Fund|Energy Trust of Oregon|Clinton Foundation|Building Decarbonization Coalition|Southwest Energy Efficiency Project|Forth Mobility Fund|Client Earth|Climate Imperative|Development Bank of Southern Africa|Boulder County, Colorado|American Council on Renewable Energy|International Center for Appropriate & Sustainable Technology|2 Degrees Investing Initiative|Power for All|Tri-State Generation and Transmission Association|Solar Energy International|Climate Solutions|CENTER for BIOLOGICAL DIVERSITY|The Energy Coalition|Wuppertal Institute|Southwest Power Pool Inc.|The Aspen Institute|Electrification Coalition|Appalachian Voices|East Bay Community Energy|Physicians, Scientists, and Engineers for Healthy Energy|Resources for the Future|Southface Energy Institute|Atlantic Council|Port of Seattle|Midwest Energy Efficieny Alliance|Association for Energy Affordability|Utah Clean Energy|Interstate Renewable Energy Council|C2ES|Environmental and Energy Study Institute|Institute For Governance And Sustainable Development|Silicon Valley Clean Energy|International Living Future Institute|Global Energy Alliance for People & Planet|American Society of Heating, Refrigerating and Air-Conditioning Engineers|Clean Energy Group|Energy Foundation China - EFC|Eugene Water & Electric Board (EWEB)|Southern Alliance for Clean Energy|E4TheFuture|Energy Web Foundation|APPA|Global Strategic Communications Council|Green and Healthy Homes Initiative|National Wildlife Federation|EIT Climate-KIC|Tempest/Sea Change Foundation|North Carolina Sustainable Energy Association|U.S. Climate Alliance|Southern Environmental Law Center|Citizens Utility Board of Illinois|Plug In America|Seventhwave|San Francisco Public Utilities Commission|Earth Advantage Institute|Institute for Transportation and Development Policy - India|Urban Green Council|Global Maritime Forum|Institute for Sustainable Communities|Urban Sustainability Directors Network|Sequoia Climate Foundation|Center for Resource Solutions|City of Chicago|Kaiser Permanente|National Audubon Society (DC)|ShareAction|Smart Freight Centre|WE ACT for Environmental Justice|Winrock International|Blue Planet Foundation|Southeast Energy Efficiency Alliance|ClearPath Foundation|Consortium for Energy Efficiency|Prayas|Metro Vancouver|The Pew Charitable Trusts|Walking Mountains Science Center|Rockefeller Brothers Fund|Climate Leadership Initiative|IKEA Foundation|Building Performance Institute|Global Environment Facility|Responsible Steel|The Greenlining Institute|We Mean Business Coalition|The Climate Center|RENEW Wisconsin|Architecture 2030|Caribbean Development Bank|Generation180|IIGCC|NW Energy Coalition|Niskanen Center|North Central Texas Council of Governments|Our Energy Policy|The Oxford Institute of Energy Studies, University of Oxford|Heising-Simons Foundation|Clean Energy Works|Clean Coalition|Emerald Cities Collaborative|League of Conservation Voters|Selco Foundation|Advanced Energy United|SPEER|Build It Green|Clean Wisconsin|International Solar Alliance|Acadia Center|City of Boise|Green Building Certification Institute|Local Government Commission|StopWaste.org|Bezos Earth Fund|McKnight Foundation|San Diego County Regional Airport Authority|The William and Flora Hewlett Foundation|Open Society Foundations|Community Office for Resource Efficiency - CORE|Climate Action Campaign|Wisconsin Energy Conservation Corporation (WECC)|Chatham House|Climate Catalyst|Energy Storage Association (ESA)|Environmental Integrity Project|Gas Technology Institute|Just Transition Fund|Natural Resource Governance Institute|Sonoma Clean Power|SouthSouthNorth|Valo Ventures|Acumen Fund|Center for Global Development|Chesapeake Climate Action Network|Climate Chance|Keystone Energy Efficiency Alliance|National Electrical Manufacturers Association (NEMA)|Natural Resources Defense Council (China)|Third Way|ACI North America|Oxfam|Prospect Silicon Valley|American Institute of Architects|Aspen Community Foundation|Northeast States for Coordinated Air Use Management (NESCAUM)|Pecan Street Inc.|Ashden|Centre for Policy Research|Citizens Utility Board of Oregon|Food & Water Watch|Habitat for Humanity|Information Technology & Innovation Foundation (ITIF)|Integrated Research and Action for Development|Mighty Earth|National Consumer Law Center|New York City Environmental Justice Alliance|Tompkins County Planning Dept|World Steel Association|Building Electrification Initiative|CARILEC|Center for Clean Air Policy|Center for Climate Protection|Community Environmental Council|Energy Outreach Colorado|Institute for Climate Economics|KAPSARC|National League of Cities|New England Clean Energy Council|Ohio Environmental Council|Overseas Development Institute|San Francisco Bay Area Planning and Urban Research Association|Social Alpha, Urban Livability Team|Solar1|Transport & Environment|Clean Air Council|Climate Arc|MiQ Foundation|The Grantham Foundation for the Protection of the Environment|The Schmidt Family Foundation|Climate Cabinet|Energy and Policy Institute|The Utility Reform Network|Aspen Center for Environmental Studies|Conservation Colorado|Eden Housing|As You Sow|European Environmental Citizens Organisation for Standardisation (ECOS)|New York Public Interest Research Group (NYPIRG)|Spark Northwest|WPPI Energy|Citizens Climate Lobby|Clean Energy Canada|Consumer Unity & Trust Society|Forum for the Future|Institute for Local Self-Reliance|International Institute for Environment and Development|Pacific Environment|Rainforest Action Network|The Paulson Institute|Unified Port of San Diego|XPRIZE|thepublicinterestnetwork|Center for Disaster Philanthropy|Energy Saving Trust-UK|Global Methane Hub|Sall Family Foundation|Rockefeller Philanthropy Advisors|The Robert Wood Johnson Foundation|Africa Minigrid Developers Association|Bonneville Environmental Foundation|Climate Policy Initiative|Enterprise Community Partners|High Tide Foundation|National Housing Trust|The Gordon E. and Betty I. Moore Foundation|AAA Initiative|American Municipal Power|Argosy Foundation|Caribbean Climate Smart Accelerator|Citizens Action Coalition of Indiana|Climate + Energy Project|Environment America|Growald Climate Fund|Integration|Kresge Foundation|Mid America Regional Council|Platte River Power Authority|R Street Institute|Sightline Institute|World Green Building Council|American Trucking Associations|American Wind Energy Association|Carbon Neutral Cities Alliance (CNCA/USDN)|Citizens Utility Board of Minnesota|Clean Technology Hub|Dave Schaller Household|Global Alliance for Incinerator Alternatives (GAIA)|Green Building Alliance|Initiative for Energy Justice|International Institute for Energy Conservation|Majority Action|Mercy Housing|Mountain Association|National Conference of State Legislatures|National Regulatory Research Institute|New York State Homes and Community Renewal|Observer Research Foundation|Partners HealthCare System, Inc.|Self-Help Credit Union|SolarPower Europe|Sustainable Silicon Valley|The Stanley Center for Peace and Security|The Wilderness Society|UNICEF Innovation Fund|Winneshiek Energy District|Fundacion Comunitaria de Puerto Rico|Mission Possible Partnership|Port of Los Angeles|Robertson Foundation|Smith Richardson Foundation, Inc.|Sustainable Connections|The Lemelson Foundation|VoLo Foundation|Alliance for Rural Electrification|Boulder Valley School District|Caribbean Centre for Renewable Energy and Energy Efficiency|City of Bloomfield|Clean Air Fund|Conservation International|EarthSpark International|Friends of the Earth|GridLab|HEET (Home Energy Efficiency Team)|International Petroleum Industry Environmental Conservation Association (IPIECA)|JPB Foundation|Laudes Foundation|National Institute of Building Science')
audienceDomainsAccounts[1, 'NGODomains'] <- paste("wri.org|sierraclub.org|worldbank.org|nrdc.org|edf.org|ifc.org|iea.org|theclimategroup.org|un.org|earthjustice.org|c40.org|ceres.org|aceee.org|ef.org|climateworks.org|weforum.org|raponline.org|undp.org|veic.org|tnc.org|europeanclimate.org|greenpeace.org|sepapower.org|e3g.org|theicct.org|votesolar.org|wbcsd.org|calstart.org|slipstreaminc.org|gggi.org|usgbc.org|denvergov.org|ucsusa.org|irena.org|carbontracker.org|pembina.org|iclei.org|lacity.org|iadb.org|seforall.org|energycenter.org|oecd.org|smud.org|westernresources.org|rewiringamerica.org|climateanalytics.org|eei.org|sfgov.org|rti.org|energyinnovation.org|elevateenergy.org|fresh-energy.org|eib.org|newbuildings.org|bsr.org|itri.org.tw|rockfound.org|bloomberg.org|rebuyers.org|uli.org|gridalternatives.org|imt.org|neep.org|ciff.org|neea.org|climateworksaustralia.org|climatenexus.org|naruc.org|mncee.org|ieefa.org|cityoftacoma.org|seia.org|bluegreenalliance.org|wwfus.org|watttime.org|solarunitedneighbors.org|afdb.org|csu.org|ase.org|breakthroughenergy.org|newclimate.org|ndcpartnership.org|americanprogress.org|unpri.org|gcfund.org|energytrust.org|clintonfoundation.org|buildingdecarb.org|swenergy.org|forthmobility.org|clientearth.org|climateimperative.org|dbsa.org|bouldercounty.org|acore.org|icastusa.org|2degrees-investing.org|powerforall.org|tristategt.org|solarenergy.org|climatesolutions.org|biologicaldiversity.org|energycoalition.org|wupperinst.org|spp.org|aspeninstitute.org|electrificationcoalition.org|appvoices.org|ebce.org|psehealthyenergy.org|rff.org|southface.org|atlanticcouncil.org|portseattle.org|mwalliance.org|aea.us.org|utahcleanenergy.org|irecusa.org|c2es.org|eesi.org|igsd.org|svcleanenergy.org|living-future.org|energyalliance.org|ashrae.org|cleanegroup.org|efchina.org|eweb.org|cleanenergy.org|e4thefuture.org|energyweb.org|publicpower.org|gsccnetwork.org|ghhi.org|nwf.org|climate-kic.org|tempestadvisors.org|energync.org|usclimatealliance.org|selcnc.org|citizensutilityboard.org|pluginamerica.org|seventhwave.org|sfwater.org|earthadvantage.org|itdp.org|urbangreencouncil.org|cityofpaloalto.org|globalmaritimeforum.org|sustain.org|usdn.org|sequoiaclimate.org|macfound.org|resource-solutions.org|cityofchicago.org|kp.org|audubon.org|shareaction.org|smartfreightcentre.org|weact.org|winrock.org|blueplanetfoundation.org|a2gov.org|seealliance.org|clearpath.org|cee1.org|prayaspune.org|metrovancouver.org|pewtrusts.org|walkingmountains.org|rbf.org|climatelead.org|ikeafoundation.org|bpi.org|thegef.org|responsiblesteel.org|greenlining.org|wemeanbusinesscoalition.org|theclimatecenter.org|renewwisconsin.org|architecture2030.org|caribank.org|generation180.org|iigcc.org|nwenergy.org|niskanencenter.org|nctcog.org|citizen.org|unido.org|ourenergypolicy.org|oxfordenergy.org|hsfoundation.org|cleanenergyworks.org|clean-coalition.org|emeraldcities.org|lcv.org|selcofoundation.org|advancedenergyunited.org|eepartnership.org|builditgreen.org|cleanwisconsin.org|isolaralliance.org|acadiacenter.org|cityofboise.org|gbci.org|lgc.org|stopwaste.org|bezosearthfund.org|mcknight.org|san.org|hewlett.org|opensocietyfoundations.org|aspencore.org|climateactioncampaign.org|weccusa.org|chathamhouse.org|climatecatalyst.org|energystorage.org|environmentalintegrity.org|gastechnology.org|justtransitionfund.org|resourcegovernance.org|sonomacleanpower.org|southsouthnorth.org|valoventures.org|acumen.org|cgdev.org|chesapeakeclimate.org|climate-chance.org|keealliance.org|nema.org|nrdc-china.org|thirdway.org|aci-na.org|oxfam.org|prospectsv.org|aia.org|aspencommunityfoundation.org|nescaum.org|pecanstreet.org|ashden.org|cprindia.org|oregoncub.org|fwwatch.org|habitat.org|itif.org|irade.org|mightyearth.org|nclc.org|nyc-eja.org|tompkins-co.org|worldsteel.org|beicities.org|carilec.org|ccap.org|climateprotection.org|cecmail.org|energyoutreach.org|i4ce.org|kapsarc.org|nlc.org|necec.org|theoec.org|odi.org.uk|spur.org|socialalpha.org|solar1.org|transportenvironment.org|cleanair.org|climatearc.org|ny.frb.org|miq.org|granthamfoundation.org|globalmethanehub.org|hightidefoundation.org|unicef.org|missionpossiblepartnership.org")

emailStats <- read_sheet('https://docs.google.com/spreadsheets/d/1HoSpSuXpGN9tiKsggHayJdnmwQXTbzdrBcs_sVbAgfg/edit#gid=450080785') %>% 
  mutate(id = as.numeric(id),
         emailName = name,
         name = paste0('NL ', date, ': ', subject))

## filter by email of interest
emailIDs <- data.frame(id = campaignEmailIDs)

## get clicks for email of interest
clicksAll <- getProspectClicks(emailIDs)

## clean and filter links
linkClicks <- clicksAll %>% 
  mutate(chart = ifelse(grepl('=chart', url), TRUE, ''),
         url = gsub('\\&utm_content=spark-b|\\&utm_content=spark-a|\\&utm_term=plain-text|\\&utm_term=chart-plain-text', '', url),
         url = sub('\\?(.*)', '', url)) %>% 
  filter(grepl(paste(methanelinks, collapse = '|'), url))

clicksByProspect <- select(linkClicks, c(emailId = list_email_id, Pardot_URL = prospect_id, url, created_at)) %>% 
  mutate(Pardot_URL = paste0("http://pi.pardot.com/prospect/read?id=", as.character(Pardot_URL)),
         CreatedDate = as.Date(created_at, format="%Y-%m-%d")) %>% 
  left_join(prospects, by = c('Pardot_URL')) %>% 
  mutate(Status = 'Email',
         Domain = sub("(.*)\\@", "", Email),
         EngagementType = 'Newsletter Click') %>% 
  left_join(select(domainKey, c(domain, domainCount = count, domainName = name, domainLevel = level)), by = c('Domain' = 'domain')) %>% 
  left_join(select(emailStats, c(emailId = id, emailName)), by = 'emailId') %>% 
  mutate(CampaignName = gsub('Spark', '', emailName), Giving_Circle = '', Top_Philanthropic = '', Last_Gift = '') %>% 
  filter(!is.na(Domain)) %>% 
  left_join(select(all_accounts, c(AccountsName = Account, Domain = AccountDomain, Industry, AccountType, TotalGiving, 
                                   NumGifts, NumOpenOpps, AccountId, DB_IndustryCategory, NAICS1, SIC1)), 
            by = c('Domain')) %>% 
  mutate(Account = ifelse(grepl('unknown|[[Unknown]]|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account),
         Account = ifelse(Account == 'Unknown' | !is.na(AccountsName), AccountsName, Account)) %>% 
  select(CampaignName, EngagementType, Id, RecordType, Status, CreatedDate, Name, Title, Domain, Email, Account, AccountType, Industry,
         TotalGiving, NumGifts, NumOpenOpps, Pardot_Score, Pardot_LastActivity, Pardot_URL, Country, State, City, Giving_Circle, Top_Philanthropic, Last_Gift, AccountId)

##

df <- campaignMembersEvents %>% 
  rbind(campaignMembersReports) %>% 
  mutate(CreatedDate = as.Date(CreatedDate, format = "%Y-%m-%d"),
         Status = ifelse(grepl('Register', Status), 'Registered - DNA', Status)) %>% 
  select(CampaignName, EngagementType, Id, RecordType, Status, CreatedDate, Name, Title, Domain, Email, Account, AccountType, Industry, 
         TotalGiving, NumGifts, NumOpenOpps, Pardot_Score, Pardot_LastActivity, Pardot_URL, Country, State, City, Giving_Circle, Top_Philanthropic, Last_Gift, AccountId) %>% 
  filter(grepl('Register|Attended|Download|Email', Status) & !is.na(Domain)) %>% 
  rbind(clicksByProspect) %>% 
  left_join(select(all_accounts, c(AccountsName = Account, Domain = AccountDomain, AccountsIndustry = Industry, AccountType2 = AccountType)), by = c('Domain')) %>% 
  mutate(Account = ifelse(grepl('unknown|[[Unknown]]|not provided|contacts created', tolower(Account))|Account == '', 'Unknown', Account),
         Industry = ifelse(Account == 'Unknown' | !is.na(Industry), AccountsIndustry, Industry),
         AccountType = ifelse(Account == 'Unknown' | !is.na(Industry), AccountType2, AccountType),
         Account = ifelse(Account == 'Unknown' | !is.na(AccountsName), AccountsName, Account)) %>% 
  select(-c('AccountsName', 'AccountsIndustry')) %>% 
  mutate(AccountType = ifelse(!is.na(AccountType), AccountType, 
                              ifelse(grepl('\\.gov|\\.co\\.us', Domain), 'Government', 
                              ifelse(grepl('org$', Domain) & !grepl('rmi\\.org|third-derivative', Domain), 'Organization', 
                                     ifelse(grepl('edu$', Domain), 'Academic', 
                                            ifelse(grepl('Household', Account), 'Household', AccountType)))))
  ) %>% 
  left_join(select(domainKey, c(govDomain = domain, level, govName = name)), by = c('Domain' = 'govDomain')) %>%
  mutate(Audience1 = ifelse(level == 'FEDERAL', 'National Gov.',
                            ifelse(level == 'STATE'|grepl('State of', Account), 'State Gov.',
                                   ifelse(level == 'LOCAL'|level == 'COUNTY', 'Local Gov.',
                                          ifelse(level == 'INTERNATIONAL', 'International Gov.',
                                                 ifelse(Domain == audienceDomainsAccounts[1, 'multilateralDomains']|grepl(audienceDomainsAccounts[1, 'multilateralAccounts'], Account), 'Multilateral Institution',
                                                        ifelse(Domain == audienceDomainsAccounts[1, 'NGODomains']|grepl(audienceDomainsAccounts[1, 'NGOAccounts'], Account), 'NGO',
                                                               ifelse(Domain == audienceDomainsAccounts[1, 'financialDomains']|grepl(audienceDomainsAccounts[1, 'financialAccounts'], Account), 'Financial Entity', 
                                                                      ifelse(grepl('Corporate', AccountType), 'Other Corporate', ''))))))))) %>%
  mutate(Audience1 = ifelse(grepl('Corporate', AccountType) & (Audience1 == ''|is.na(Audience1)), 'Other Corporate', Audience1),
         Audience1 = ifelse(grepl('Foundation', AccountType), 'Foundation', Audience1),
         Audience1 = ifelse(grepl('state of|commonwealth of', tolower(Account)), 'State Gov.', Audience1),
         Audience1 = ifelse(grepl('city of|county of', tolower(Account)), 'Local Gov.', Audience1),
         Account = ifelse(grepl('RMI', Account), 'Household', Account),
         Audience1 = ifelse(grepl('Academic', AccountType) & (Account != '' | is.na(Account) | Account != 'Unknown'), 'Academic', Audience1),
         Audience1 = ifelse((is.na(Audience1) | Audience1 == '') & grepl('Foundation', AccountType), 'Foundation', Audience1),
         DonorType = ifelse(!is.na(Giving_Circle) & Giving_Circle != '', 'Giving Circle',
                        ifelse(!is.na(Last_Gift) & Last_Gift != '', 'Donor', NA)),
         Icon = ifelse(EngagementType == 'Report Download', 1, 
                       ifelse(EngagementType == 'Event', 2, 
                              ifelse(EngagementType == 'Newsletter Click', 3, NA))),
         ) %>%
  relocate(Audience1, .after = AccountType) %>%
  mutate(Audience2 = ifelse(grepl('Gov', Audience1), 'Government', Audience1)) %>% 
  relocate(Audience2, .after = Audience1) %>% 
  mutate(govName = sub(" \\|(.*)", "", govName),
         govName = sub(" \\-(.*)", "", govName),
         Account = ifelse(Account == 'Unknown' & !is.na(govName), govName, Account),
         Pardot_ID = sub("(.*)=", "", Pardot_URL)) %>% 
  distinct(Id, CampaignName, .keep_all = TRUE) %>% 
  mutate(DownloadTF = ifelse(grepl('Download', EngagementType), 1, NA),
         EventTF = ifelse(grepl('Event', EngagementType), 1, NA),
         EmailClickTF = ifelse(grepl('Click', EngagementType), 1, NA))

###
donors <- df %>% 
  select(Pardot_ID, Name, DonorType, EngagementType, CampaignName, CreatedDate, Id, Pardot_Score, NumGifts, TotalGiving, Account) %>% 
  distinct() %>% 
  filter(!is.na(DonorType))

getDonations <- function(){
  
  allOpportunities <- data.frame(DonationID = '', DonationDate = '', DonationValue = '', Pardot_ID = '', CreatedDate = '',
                                 EngagementType = '', CampaignName = '', Pardot_Score = '',
                                 NumGifts = '', TotalGiving = '', Account = '', Name = '', DonorType = '')[0,]
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
          cbind(donors[i, 'CreatedDate']) %>% 
          mutate(Pardot_ID = paste(donors[i, 'Pardot_ID']),
                 Id = paste(donors[i, 'Id']),
                 DonationDate = as.Date(DonationDate, format = "%Y-%m-%d"),
                 CreatedDate = as.Date(CreatedDate, format="%Y-%m-%d"),
                 EngagementType = paste(donors[i, 'EngagementType']),
                 CampaignName = paste(donors[i, 'CampaignName']),
                 Pardot_Score = paste(donors[i, 'Pardot_Score']),
                 NumGifts = paste(donors[i, 'NumGifts']),
                 TotalGiving = paste(donors[i, 'TotalGiving']),
                 Account = paste(donors[i, 'Account']),
                 Name = paste(donors[i, 'Name']),
                 DonorType = paste(donors[i, 'DonorType'])) 
        allOpportunities <- allOpportunities %>% rbind(VA)
      }
    }, error = function(e){ NA } )
    
  }
  return(allOpportunities)
}

allOpps <- getDonations() 

ap <- allOpps %>% 
  group_by(DonationId) %>% 
  summarize(count = n())

allOpps2 <- allOpps %>% 
  filter(DonationDate > CreatedDate) %>% 
  mutate(DonationValue = as.numeric(DonationValue),
         difftime = difftime(DonationDate, CreatedDate, units = "days"),
         difftime = as.numeric(gsub("[^0-9.-]", "", difftime)),
         DiffTime = ifelse(difftime < 31, 1, difftime),
         AttributtedValue = ifelse(DonationValue / (1/(1.00273-0.00273*DiffTime)) < 0, 0, DonationValue / (1/(1.00273-0.00273*DiffTime)))) %>% 
  relocate(DonationValue, .before = AttributtedValue) %>% 
  left_join(ap) %>% 
  mutate(AttributtedValue = round(AttributtedValue/count, 1))

allOppsProspect <- allOpps2 %>% 
  group_by(Id, CampaignName) %>% 
  summarize(AttributtedDonationValue = sum(AttributtedValue))


df2 <- df %>% 
  left_join(allOppsProspect) %>% 
  mutate(Engagements = 1,
         GivingCircleTF = ifelse(Giving_Circle == ''|is.na(Giving_Circle), NA, 1),
         OpenOppTF = ifelse(NumOpenOpps == ''|NumOpenOpps == 0|is.na(NumOpenOpps), NA, 1),
         DonorTF = ifelse(DonorType == ''|is.na(DonorType), NA, 1),
         LastGift = as.numeric(Last_Gift),
         LapsedDonorsTF = ifelse((LastGift > 549 & LastGift < 1825), 1, NA)) %>% 
  select(CampaignName, EngagementType, Icon, Id, RecordType, Status, EngagementDate = CreatedDate, Name, Title, Domain, Email, 
         LastGift, DonorType, AttributtedDonationValue, AccountId, Account, AccountType, Audience1, Audience2, Industry, 
         AccountTotalGiving = TotalGiving, NumGifts, NumOpenOpps, 
         Pardot_URL, Pardot_ID, GivingCircleTF, OpenOppTF, DonorTF, LapsedDonorsTF, DownloadTF, EventTF, EmailClickTF, Engagements) %>% 
  mutate(Account = ifelse(grepl('Household', Account)|AccountType == 'Household', 'Household', Account))

print('writing df')
write_sheet(df2, ss = ss, sheet = 'Salesforce - All')

print('writing donation conversions')
write_sheet(allOpps2, ss = ss, sheet = 'Salesforce - Donations')

