library(data.table)
library(stringr)
sample_zhq<-fread('C:\\Users\\fengyu02\\Downloads\\SampleData.txt',sep='|')

sample_zhq[grepl('Subject',TEXT),date:=sub('Subject:.*/([ 0-9]*\\.[0-9]*\\.[0-9]*)/.*','\\1',TEXT)]
sample_zhq[grepl('脰梅脤芒.*Daily Report',TEXT),date:=sub('.*/([ 0-9]*\\.[0-9]*\\.[0-9]*)/.*','\\1',TEXT)]

sample_zhq[1,Email_ID:=1]
for(i in 2:nrow(sample_zhq)){
  if(sample_zhq[i,Flag]==sample_zhq[i-1,Flag] & !grepl('From|路垄录镁脠脣',sample_zhq[i,TEXT])){
    sample_zhq[i,Email_ID:=sample_zhq[i-1,Email_ID]]
  }else{
    sample_zhq[i,Email_ID:=sample_zhq[i-1,Email_ID]+1]
  }
}
# sample_zhq[grep('pipeline',TEXT),.N,by='Email_ID'][N>1]

fill_date = sample_zhq[!is.na(date),.(date,Email_ID)]
sample_zhq_assigndate = merge(sample_zhq[,.(Email_ID,TEXT,Flag,Date)],
                              fill_date,
                              on = 'Email_ID',
                              all.x = T,
                              suffixes = c('_temp','_final'))
full_list =1:1304
full_list[!full_list %in% sample_zhq_assigndate[grepl('Miss',TEXT)]$Email_ID]

# sample_zhq_assigndate[Email_ID==847]

sample_zhq_assigndate[,TEXT:=str_squish(TEXT)]
sample_zhq_assigndate[,date:=str_squish(date)]
# sample_zhq_assigndate[Email_ID==107]
# sample_zhq_assigndate[grep('路垄录镁脠脣',TEXT)]




clean_email<-function(i,sample_zhq_assigndate){
  test = sample_zhq_assigndate[Email_ID==i]
  # test = test[TEXT!='']
  test[,row_no:=.I]
  data_date = as.numeric(strsplit(unique(test$date),'\\.')[[1]])
  data_date = formatC(data_date, width = 2, flag = "0")
  test[,date:=paste0(data_date[c(3,2,1)],collapse = '-')]
  
  start_search = test[grep('Miss.*sales',TEXT)]$row_no
  end_search = test[grep('pipeline',TEXT)]$row_no
  
  if(sum(grepl('NO',toupper(test[grep('Miss',TEXT)]$TEXT)))>0| sum(grepl('Miss',test$TEXT))==0|nrow(test[row_no>start_search&row_no<end_search][grep('^G',TEXT)])==0){
    result = unique(test[,.(Email_ID,Flag,date)])
    result[,`:=`(ID=NA,
                 Description='NO Missed sales',
                 Price=NA)]
    return(result)
  }
  # test = test[row_no>start_search &row_no<end_search]

  result = test[grep('^G[G34|G33|G36|G37|G38|G0A|GoA|GOA]',TEXT)][row_no>start_search &row_no<end_search][!grep('Go to|Gouverneur',TEXT)]
  rows = result$row_no
  title_end = min(test[grep('^G',TEXT)][row_no>start_search &row_no<end_search]$row_no)
  
  title = test[row_no>start_search& row_no<title_end]$TEXT
  title = title[title!='']
  title[title=='隆隆']='Value'
  price_position = grep('value',tolower(title))-1
  reason_position = grep('reason',tolower(title))-1
  
  rows_temp = c(rows,end_search)
  rows_gap = rows_temp[2:length(rows_temp)]-rows[1:(length(rows_temp)-1)]
  if(sum(c(rows_gap==1))>0){
    rows[c(rows_gap==1)] = rows[c(rows_gap==1)]+1
    result$row_no = rows
  }
  rows_nospace=c(rows[which(rows_gap-length(title)==0)])
  rows_space = rows[which(rows_gap-length(title)>0)]
  if(length(rows_nospace)>0){
    result[row_no %in% rows_nospace,Description:=test[rows_nospace+reason_position]$TEXT]
    result[row_no %in% rows_nospace,Price:=test[rows_nospace+price_position]$TEXT]
    
  }
  if(length(rows_space)>0){
    expand = data.table(result[row_no %in% rows_space]$row_no)
    ind = data.table(rows_space,
                     test[TEXT!='']$row_no[which(test[TEXT!='']$row_no %in% rows_space)+price_position])
    price_map = merge(test[TEXT!=''],
          ind,
          by.x='row_no',
          by.y = 'V2',
          all.y = T)
    price_map = right_join(price_map,
                      expand,
                      by=c('rows_space'='V1'))
    ind = data.table(rows_space,
                     test[TEXT!='']$row_no[which(test[TEXT!='']$row_no %in% rows_space)+reason_position])
    reason_map = merge(test[TEXT!=''],
                      ind,
                      by.x='row_no',
                      by.y = 'V2',
                      all.y = T)
    reason_map = right_join(reason_map,
                           expand,
                           by=c('rows_space'='V1'))
    result[row_no %in% rows_space,Description:=reason_map$TEXT]
    result[row_no %in% rows_space,Price:=price_map$TEXT]
  }
  result[,Price:=gsub('\\?','',str_trim(Price))]
  setnames(result,'TEXT','ID')
  result[,`:=`(row_no=NULL,
               Date=NULL)]
  
  # result[,date:=paste0(data_date,collapse = '-')]
  return(result)
}
# test = clean_email(1,sample_zhq_assigndate)
df=NULL
for(i in 1:1304){
  temp = clean_email(i,sample_zhq_assigndate)
  df=rbind(df,temp)
}
fwrite(df,'C:\\FYT\\FYT\\Training\\DAR\\WR\\ZHQ\\Missed_sales.csv',row.names = F)
fwrite(sample_zhq_assigndate,'C:\\FYT\\FYT\\Training\\DAR\\WR\\ZHQ\\Cleaned_sample_data.csv',row.names = F)

# library(parallel)
# cl.core<-detectCores()
# cl <- makeCluster(cl.core-1)
# clusterExport(cl,
#               varlist =c('sample_zhq_assigndate'))

