import logging
import myConstants as mc
import paramiko as para
import glob
import extn_utils as eut
import utils as ut
import fnmatch #provides support for Unix shell-style wildcards
import send_emails as se


FGCOLOR=mc.FGCOLOR

def getEceAllFiles(targetFolder,filePattern,eceFiles):
     '''
checks and parses the last uploaded item file's session number.
uses while loop to get upto 7 consecutive session numbers and replaces new session number in the ece filenames.
adds all the new files to be downloaded from the server to eceAlLFiles list.
     '''
     all_file = glob.glob(f'{targetFolder}/{filePattern}')
     sessionNum = int(max(all_file)[-8:-4])
     startSession = (sessionNum+1)
     endSession = (startSession + 2)
     eceAllFiles=[]
     print('Last downloaded session number:' ,sessionNum)
     while startSession <= endSession:
        for ecefile in eceFiles:
           ecefile = ecefile.replace('{startSession}',str(startSession))           
           eceAllFiles.append(ecefile)
        startSession += 1 
     #print(eceAllFiles)
     return eceAllFiles

def downloadFiles():
    downloadedeceFile = ''
    pics_config = ut.load_json("resources/extn/picsProcessor.json")
    if pics_config['process'] is True:
      targetFolder = pics_config['targetFolder']
      sourceFolder = pics_config['sourceFolder']
      filePattern = pics_config['filePattern']
      eceFiles = pics_config['eceFiles']

      #get a list of eceFiles that needs to be downloaded from the server.
      eceAllFiles = getEceAllFiles(targetFolder,filePattern,eceFiles)
    try:
        # connect to SSH and then sftp
        ssh = para.SSHClient()
        ssh.set_missing_host_key_policy(para.AutoAddPolicy())
        ssh.connect(hostname='172.18.102.3', username='shristiamatya', password='GrindelSpring2024!', port=22)
        sftp_client = ssh.open_sftp()
        # pick one file at a time from the list of eceAllFiles and get it from the source folder.
        for eceFile in eceAllFiles:
            eceFilenameCopy = eceFile
            sourceFile = f'{sourceFolder}/{eceFile}'
            targetFile = f'{targetFolder}/{eceFile}'
            try:
                # checks if the source file exists in the sftp
                if sftp_client.stat(sourceFile) is not None:
                    print('Getting file:', eceFile)
                    downloadedeceFile = eceFile;
                    sftp_client.get(sourceFile, targetFile)
                # parse the session number from the file.
                if pics_config['parseSessionNum'] in eceFile:
                    activeSession = eceFile[15:19]
                   # newSessionNums.append(activeSession)
            except FileNotFoundError:
                print(f'File Not Found:{eceFilenameCopy}')
        sftp_client.close()
        ssh.close()
       # print(downloadedeceFile)
    except:
        print(f"{FGCOLOR}Check your pics server account/password or your access.")
    return downloadedeceFile
