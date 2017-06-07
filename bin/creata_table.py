#!/usr/bin/python

import sys
import os
def getpath():
    filepath=os.path.dirname(os.path.realpath(__file__))
#    path=os.path.dirname(path)
    path=os.path.abspath(os.path.join(filepath, os.path.pardir))
#    path3=os.path.join(path,os.path.pardir)
#    path4=os.path.join(path2,'bin/vv')
#    os.system('echo '+path4)
#    print '11'
    return path

def createtable(path):
    scriptpath=os.path.join(path,'engines/hive/population/hiveCreateLoad.sql')
    os.system('source '+scriptpath)

if __name__ == '__main__':
    projectpath=getpath()
    createtable(projectpath)
