# -*- coding:UTF-8 -*-
from com.um.ykang.mission.Constance import Constance
import os
import json
from com.um.ykang.lookalike.spark import getQuserInLast5Monthly,\
    getUserOpenPackageWeeklyByGivenQpackage
from com.um.ykang.lookalike.local.getQuserOpenPackage import getQuserOpenPackage
from com.um.ykang.lookalike.entity.Qpackage import Qpackage
from com.um.ykang.lookalike.entity.Candidate import Candidate
from com.um.ykang.lookalike.entity.Quser import Quser
from com.um.ykang.data.format.File import LineFile, SepFile
from com.um.ykang.mission.MissionConf import MissionConf
from com.um.ykang.mission.MissionContext import MissionContext
from com.um.ykang.lookalike.model.GBClassifierModeling import GBClassifierModeling
from com.um.ykang.util.BashUtil import BashUtil

if __name__ == '__main__':
    with open('/home/hhx/hhxgit/look-alike-new/look-alike-new/look-alike-new/src/com/um/ykang/lookalike/spark/paramsConf.json') as json_data:
        paramsConf = json.load(json_data)
    BASE_PATH = paramsConf['BASE_PATH']
    DATA_BASE_PATH = os.path.join(BASE_PATH,'data')
    Constance.WORK_SPACE = os.path.join(BASE_PATH, 'workspace')
    Constance.SCRIPT_S3_PATH = paramsConf['SCRIPT_S3_PATH']
    Constance.SUBMIT_PY_GIT_PATH = paramsConf['SUBMIT_PY_GIT_PATH']
    
    payQuserS3Path = paramsConf['payQuserS3Path']
    qUserInLast5EachDayS3Path = paramsConf['qUserInLast5EachDayS3Path']
    tfBeginDay = paramsConf['tfBeginDay']
    tfInterval = paramsConf['tfInterval']
    tfIsForward = paramsConf['tfIsForward']
    qPackageDictS3Path = paramsConf['qPackageDictS3Path']
    userOpenPackageWeeklyByGivenQpackageS3Path = paramsConf['userOpenPackageWeeklyByGivenQpackageS3Path']
    candidateBeginDay = paramsConf['candidateBeginDay']
    candidateInterval = paramsConf['candidateInterval']
    candidateIsForward = paramsConf['candidateIsForward']
    trainQuserBeginDay = paramsConf['trainQuserBeginDay']
    trainInterval = paramsConf['trainInterval']
    trainIsForward = paramsConf['trainIsForward']
    np_factor = float(paramsConf['neg_pos_factor'])
def setPath():
    Candidate.BASE_PATH = os.path.join(DATA_BASE_PATH, 'candidatesInfo')
    Qpackage.QPACKAGE_ID_TXT = os.path.join(DATA_BASE_PATH,'dict','qPackageToId.txt')
    Quser.QUSER_OPENPACKAGE_PATH = os.path.join(DATA_BASE_PATH,'qUserOpenPackageInfo')
    
def makeFolder():
    if not os.path.exists(Constance.WORK_SPACE):
        os.makedirs(Constance.WORK_SPACE)
    if not os.path.exists(DATA_BASE_PATH):
        os.makedirs(DATA_BASE_PATH)
        os.mkdir(os.path.join(DATA_BASE_PATH,'dict'))
        os.mkdir(os.path.join(DATA_BASE_PATH, 'candidatesInfo'))
        os.mkdir(os.path.join(DATA_BASE_PATH, 'qUserOpenPackageInfo'))
        os.mkdir(os.path.join(DATA_BASE_PATH, 'predict'))

if __name__ == '__main__': 
    setPath() 
    makeFolder()
    mconf = MissionConf().setAppName('main')
    msc = MissionContext(conf=mconf)
    [_, appPath] = msc.getFolder()  #?
#             
    BashUtil.s3Cp(os.path.join(appPath, 'qUserOrigin.txt'), dst=os.path.join(payQuserS3Path, 'qUserOrigin.txt'), recursived=False)  #?
    #从last5中计算一个月的优质用户行为
    getQuserInLast5Monthly.runJob('getQuserInLast5Monthly', payQuserS3Path,
                                qUserInLast5EachDayS3Path,
                                tfBeginDay,tfInterval,tfIsForward)
      
    #将getQuserInLast5Monthly的结果下载到本地计算tf，并将优质用户打开的所有包上传到S3
    getQuserOpenPackage(qUserInLast5EachDayS3Path,
                         tfBeginDay, tfInterval, tfIsForward,
                         qPackageDictS3Path, isDownload=True)
     
    #获得Candidates一周的数据，用qPackage过滤
    getUserOpenPackageWeeklyByGivenQpackage.runJob('getUserOpenPackageWeeklyByGivenQpackage', qPackageDictS3Path, 
                                                   userOpenPackageWeeklyByGivenQpackageS3Path, 
                                                   candidateBeginDay, candidateInterval, candidateIsForward)
      
    BashUtil.s3Cp(userOpenPackageWeeklyByGivenQpackageS3Path, Candidate.BASE_PATH, recursived=True)
      
    gbModel = GBClassifierModeling('GBCModeling', Quser.QUSER_OPENPACKAGE_PATH, Candidate.BASE_PATH, Qpackage.getQpackageToId())
    #gbModel.crossValid(np_factor, max_depth_=5)
    gbModel.train(neg_pos_factor=np_factor, max_depth_=5)
    postiveUser = gbModel.predict(candPath=os.path.join(DATA_BASE_PATH, 'predict'), num_take=10, each_take=100)
    f = LineFile().open(srcPath=os.path.join(appPath, 'postiveUsers.txt'), mode='txt', flag='w')
    for pUser in postiveUser:
        f.writeLine(pUser)
    f.close()
    
    
    
