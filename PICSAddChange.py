"""
Created on Thu May 18 11:39:58 2023
Extracts the attachment that comes to the inbox; subject: PICS 4PL Active Item Adds/Updates
Stores the attachment to the folder.
"""
import datetime
import os
import openpyxl
import send_emails_smtp as se
import pandas as pd
import extn_utils as extn
import common as c
import base64
from googleapiclient.errors import HttpError
from datetime import date, timedelta
import utils as ut
import pandas_etl as pe
import glob

allVendorsDF = pd.DataFrame();
requiredEDDs = ['NF01','NFAA','NCAB','OSAA','AFAA','AFAB','MLAB','MLAA']
#get the last downloaded attachment
today = date.today()
if today.weekday() == 0:
    yesterday = today - timedelta(days=3)
else:
   yesterday = today - timedelta(days=1)
query ="after: {} subject: {}".format(yesterday.strftime('%Y/%m/%d'),'PICS 4PL Active Item Adds/Updates')
filenameList=[];
#filenameList = ['downloadedAttachments/PICS-4PL-New-&-Updated-Items-LCI-02-17-2026.xlsx','downloadedAttachments/PICS-4PL-New-&-Updated-Items-BISM-02-17-2026.xlsx']
'''
Extracts the attachment that comes to GSSAutomation group email; subject: [GSSAutomation] Awards in error report
Stores the attachment to the folder.
'''
def getAttachmentFromInbox():
    try:
        service =c.gmail_authenticate()
        messages = c.search_messages(service, query)
        print(messages)
    
        for message in messages:
           result = service.users().messages().get(userId = 'me',id= message['id']).execute()
           emailParts = result['payload']['parts']
           for parts in emailParts:
               filename = parts['filename']
               if filename :
                  attn_id = parts['body']['attachmentId']
                  getAttachment = service.users().messages().attachments().get(userId='me',messageId=message['id'],id = attn_id).execute()
                  data = getAttachment['data']
                  file_data=base64.urlsafe_b64decode(data.encode('UTF-8'))
                  attachFolder = f'downloadedAttachments/{parts["filename"]}'
                  with open(attachFolder,'wb') as f:
                      f.write(file_data)
                  print('Attachment saved:', filename) 
                  filenameList.append(attachFolder)
        return filenameList
    except HttpError as error:
            print(f'An error occurred: {error}')


'''
For each attachment, load the excel attachments as dataframe
see the item number and sort out depending on the agencies
then check if the item number already exist in the pics table. If yes, then it's a change else it is add.

'''
def loadTheAttachments(filenameList):
    global allVendorsDF;
    print(filenameList);
    for vendorFile in filenameList:

        os.environ['fileName'] = vendorFile
        config = ut.load_json("resources/extn/excelToDF.json")
        if config is not None:
          etl = pe.PandasEtl(config)
          store_df = etl.from_source()
          allVendorsDF = pd.concat([allVendorsDF, store_df], ignore_index=True)
          allVendorsDF['Edd Prefix'] = allVendorsDF['Item Number'].str[:4];
    reqVendorsDf=allVendorsDF[allVendorsDF['Edd Prefix'].isin(requiredEDDs)]
    return reqVendorsDf

'''
Extract attachment that comes everyday in an email subject- [GSSAutomation] Awards in error report
Insert the CSV file to the table.
'''

def executequery():
    if len(allVendorsDF) > 0 :
       allitemno = "('" + "'),('".join(reqVendorsDf['Item Number']) + "')"
       sqlquery = "WITH allItemNos AS (SELECT [Item Number] FROM (VALUES " + allitemno + ") AS T([Item Number])) ,concatIMItemno as (select ITEMNO,concat(projcode COLLATE DATABASE_DEFAULT,ITEMNO COLLATE DATABASE_DEFAULT) as ITEMNO2,processdate, [action] from QS_QUERY.dbo.PICS_ITEM_MAPPING) ,maxProcessDate as (select a.[Item Number] as ITEMNO,max(PROCESSDATE) as PROCESSDATE from concatIMItemno t right join allItemNos a on t.ITEMNO2= a.[Item Number] group by a.[Item Number] ) ,deletesFromIM as ( select c.itemno, case when [ACTION] ='D' then 'Delete' else null end as [Action] from concatIMItemno cin right join maxProcessDate c on c.ITEMNO = cin.ITEMNO2 and c.PROCESSDATE=cin.PROCESSDATE) Select d.ITEMNO as [Item Number], case when [Action] is not null then [Action] when [Action] is null and PICSDATE <> '' then 'Change' else 'Add' end as [Item Add or Change] from deletesFromIM d left join PICS_CATALOG p on d.ITEMNO = p.[4plpartno]"
       print(sqlquery);
    else:
        print("reqVendorsDF is empty")
    return extn.executequery(sqlquery,dburl);

def createFileAndTab(attachment,filtered_df,sheet_name):
    os.environ["fileName"] = attachment
    os.environ["operation"] = sheet_name
    downloadFileConfig = ut.load_json("resources/extn/dfToExcel.json")
    if downloadFileConfig is not None:
        etl = pe.PandasEtl(downloadFileConfig)
        etl.to_destination(filtered_df)
    else:
       print("dfToExcel json file did not load properly.")

def sendemail(emailAddresses,attachment):
    finalBody = distributionList.get('emailbody')
    subject = f'{storename} Item Add/Change/Delete Report {yesterday} '
    filename = f'{storename}_{yesterday}.xlsx'
    emailAddress = distributionList.get('to')
    allCCEmailAddress = distributionList.get('cc')
    fromEmail = distributionList.get('from_replyTo')
    allBCCEmailAddress = ''
    try:
       email_params_list = [se.EmailParams(fromEmail, emailAddress, allCCEmailAddress, allBCCEmailAddress, fromEmail, subject, finalBody, [attachment], filename)]
       se.send_email_with_starttls(email_params_list)
    except Exception as e:
        extn.print_colored("Email not sent" + str(e), "red")

if __name__ == '__main__':
    try:
       extn.deleteFolderContents('./output/files')
       dbconfig = ut.load_json("resources/extn/dburl.json")
       #  print(dbconfig)
       operSys =  extn.get_os_info()
       if operSys.lower() == 'linux':
          dburl = dbconfig.get('dburl_ux')
       elif operSys.lower() == 'windows':
          dburl = dbconfig.get('dburl_win')
       os.environ["dburl"] = dburl

       filenameList = getAttachmentFromInbox() #download the recent PICS files
       if filenameList :
          reqVendorsDf=loadTheAttachments(filenameList);
          sqloutputDF = executequery();
          #print(sqloutputDF);
          df = pd.merge(allVendorsDF, sqloutputDF, on='Item Number',how='left');  # joins two dataframes on common item number
          df = df.drop(columns = ["Contract Number","BPA Number","SIN","Price Category","Sched Price","Cost Price"])
          df["Status Date"] = yesterday
          df = df[["Edd Prefix","Status Date","Vendor Name","Item Add or Change","Item Number","Item Name","Mfr Name","Part Number","UOM","Vendor Part Number","Sell Price"]]
          print(df);
          storesConfig = ut.load_json("resources/extn/stores.json")
          #storesConfig = ut.load_json("resources/extn/testStores.json")
          for store in storesConfig.values():
              for i in range(len(store)):
                  storename = store[i]['name'];
                  distributionList = store[i]['distributionList']
                  print(storename);
                  attachment = f'output/files/{storename}_{yesterday}.xlsx';
                  process = (store[i]['process']);
                  vendorPrefix = (store[i]['vendorname4plprefix']);
                  emailAddresses = (store[i]['distributionList']);
                  # print(emailAddresses)
                  if (process):
                      finalFilteredDf = pd.DataFrame(); #new dataframe after every vendor
                      for vendor in vendorPrefix:
                          for value in vendor.values():# accesses eddprefix for each vendor
                              eddPrefix = value;
                              # print(VendorName,eddPrefix)
                              if eddPrefix in df['Edd Prefix'].values:  # checks if the eddprefix is in the mergeddf
                                  filteredDf = df[df['Edd Prefix'] == eddPrefix]  # filters the rows where the eddprefix is found and puts it in a dataframe
                                  finalFilteredDf=pd.concat([finalFilteredDf,filteredDf],ignore_index=True)
                                  print(filteredDf)
                              else:
                                  print(f'{eddPrefix} not found for {storename}');
                              sheet_name = "PICS_Add_Update"
                              if finalFilteredDf.empty: #check if the finalfilteredDF is empty
                                  print("Edds not found.")
                              else:
                                 createFileAndTab(attachment, finalFilteredDf, sheet_name)
                      sendemail(emailAddresses, attachment)
                  else:
                      print(f'{process} is set to false for {storename}');
    except Exception as e:
        print("Error completing the process:" + str(e))

