"""
Created on Thu May 18 11:39:58 2023
@author: ShristiAmatya, ChatGPT
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
#get the last downloaded attachment
today = date.today()
if today.weekday() == 0:
    yesterday = today - timedelta(days=3)
else:
   yesterday = today - timedelta(days=1)
query ="after: {} from: {} subject: {}".format(yesterday.strftime('%Y/%m/%d'),'pics-do-not-reply@gsa.gov','PICS 4PL Active Item Adds/Updates')
filenameList=[];

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

'''
Extract attachment that comes everyday in an email subject- [GSSAutomation] Awards in error report
Insert the CSV file to the table.
'''

def executequery():
    allitemno = "('" + "'),('".join(allVendorsDF['Item Number']) + "')"
    sqlquery = "WITH cte AS (SELECT [Item Number] FROM (VALUES" + allitemno + ") AS T([Item Number])) SELECT c.*, case when PICSDATE <> '' then 'Change' else 'Add' end as 'Item Add or Change' FROM cte c left join PICS_CATALOG p  on p.[4PLPARTNO] = c.[Item Number]"
    print(sqlquery);
    return extn.executequery(sqlquery);

def createFileAndTab(attachment,filtered_df,sheet_name):
    os.environ["fileName"] = attachment
    os.environ["operation"] = sheet_name
    downloadFileConfig = ut.load_json("resources/extn/dfToExcel.json")
    if downloadFileConfig is not None:
        etl = pe.PandasEtl(downloadFileConfig)
        etl.to_destination(filtered_df)
    else:
       print("dfToExcel json file did not load properly.")

def createDeleteTab(attachment,filtered_df,sheet_name):
    try:
        # Check if the sheet exists
        if os.path.exists(attachment):
            book = openpyxl.load_workbook(attachment)
            if sheet_name not in book.sheetnames:
              # If the sheet does not exist, create it
               with pd.ExcelWriter(attachment, engine='openpyxl', mode='a') as writer:
                   filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)
               print(f"Sheet '{sheet_name}' created successfully.")
            else:
                createFileAndTab(attachment, filtered_df, sheet_name)
        else:
             createFileAndTab(attachment, filtered_df, sheet_name)

    except FileNotFoundError:
        print(f"File '{attachment}' does not exist.")


def getPicsDeleteFile(picsDeleteFile):
    picsItemMappingFile = f'{picsItemMappingFile_targetFolder}/{picsDeleteFile}'
    print(picsItemMappingFile)
    columnNames = ["Action","ItemNo","Edd Prefix","Agency","BPA Number","column1","Sinno"]
    df = pd.read_csv(filepath_or_buffer=picsItemMappingFile,engine='python',delimiter=r'\^|\|',header= None);
    df_cleaned = df.dropna(axis=1,how='any')
    df_cleaned.columns = columnNames
    picsDeleteItemsDF = df_cleaned[df_cleaned['Action'] =='D']
    print(picsDeleteItemsDF)
    return picsDeleteItemsDF

def sendemail(emailAddresses,attachment):
    finalBody = distributionList.get('emailbody')
    subject = f'{storename} Item Add/Change/Delete Report {yesterday} '
    filename = f'{storename}_{yesterday}.xlsx'
    emailAddress = distributionList.get('to')
    allCCEmailAddress = distributionList.get('cc')
    fromEmail = distributionList.get('from_replyTo')
    allBCCEmailAddress = ''
    try:
       #extn.setColumnWidthDynamically(attachment)
       email_params_list = [se.EmailParams(fromEmail, emailAddress, allCCEmailAddress, allBCCEmailAddress, fromEmail, subject, finalBody, [attachment], filename)]
       se.send_email_with_starttls(email_params_list)
    except Exception as e:
        extn.print_colored("An error occurred while sending the email:" + str(e), "red")

if __name__ == '__main__':
       extn.deleteFolderContents('./output/files')
       db_config = ut.load_json("resources/extn/dbConfig.json")
       dbUsername = db_config['dbUsername']
       dbPassword = db_config['dbPassword']
       dbHostname = db_config['dbHostname']
       openSys = extn.get_os_info()
       print(openSys)
       if openSys.lower()=='linux':
           dburl = db_config['dburl_ux']
       elif openSys.lower()=='windows':
           dburl = db_config['dburl_win']
           
       os.environ['dburl'] = dburl
       filenameList = getAttachmentFromInbox()
       picsItemMappingFile_targetFolder = "./picsDownload"
       '''
       downloadedeceFile = pics.downloadFiles()
       if downloadedeceFile == '':
           filePattern = 'ece_item_mappings_geco_*.txt'
           all_files = glob.glob(f'{picsItemMappingFile_targetFolder}/{filePattern}')
           sessionNum = int(max(all_files)[-8:-4])
           downloadedeceFile = f"ece_item_mappings_geco_{sessionNum}.txt"
       '''
       if filenameList :
          loadTheAttachments(filenameList);
          sqloutputDF = executequery();
          #print(sqloutputDF);i
          df = pd.merge(allVendorsDF, sqloutputDF, on='Item Number',how='left');  # joins two dataframes on common item number
          df = df.drop(columns = ["Contract Number","BPA Number","SIN","Price Category","Sched Price","Cost Price"])
          df["Status Date"] = yesterday
          df = df[["Edd Prefix","Status Date","Vendor Name","Item Add or Change","Item Number","Item Name","Mfr Name","Part Number","UOM","Vendor Part Number","Sell Price"]]
          print(df);
          #picsDeleteItemsDF = getPicsDeleteFile(downloadedeceFile)
          storesConfig = ut.load_json("resources/extn/stores.json")
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
                      for vendor in vendorPrefix:
                          for key, value in vendor.items():  # accesses both the keys and values from the dictionary
                              VendorName = key;
                              eddPrefix = value;
                              # print(VendorName,eddPrefix)
                              if eddPrefix in df['Edd Prefix'].values:  # checks if the eddprefix is in the mergeddf
                                  filtered_df = df[df['Edd Prefix'] == eddPrefix]  # filters the rows where the eddprefix is found and puts it in a dataframe
                                  sheet_name = "PICS_Add_Update"
                                  print(filtered_df)
                                  createFileAndTab(attachment, filtered_df,sheet_name)
                              else:
                                  print(f'{eddPrefix} not found for {storename}');
                              '''    
                              if eddPrefix in picsDeleteItemsDF['Edd Prefix'].values:
                                  filteredPicsDeleteItemsDF = picsDeleteItemsDF[picsDeleteItemsDF['Edd Prefix'] == eddPrefix]
                                  filteredPicsDeleteItemsDF =filteredPicsDeleteItemsDF[["ItemNo","Edd Prefix","BPA Number"]]
                                  sheet_name = "PICS_Delete"
                                  createDeleteTab(attachment, filteredPicsDeleteItemsDF,sheet_name)
                              else:
                                  print(f'{eddPrefix} not found for PICS Delete File');
                              '''
                      sendemail(emailAddresses, attachment)
                  else:
                      print(f'{process} is set to false for {storename}');
