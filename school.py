#!/usr/bin/env python 3.5
#coding=utf-8 
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


import xlrd
import xlwt
import datetime 
import sqlite3
import types
import os 
from xlutils.copy import copy 

class schoolAsset:
    #定义基本属性
    inputFile = ""
    outputFile = ""
    sheet_Index_Jincai = 5
    sheet_Index_Banxue = 8
    conn = sqlite3.connect("school.db")
    #定义私有属性,私有属性在类外部无法直接进行访问

    #定义构造方法
    def __init__(self,infile,outfile):
        self.inputFile = infile
        self.outputFile = outfile
        self.createTable()
        self.clearTable()


    def __del__(self):
        self.conn.close

       
    def clearTable(self):
        print "clear tables ..."
        cur = self.conn.cursor()
        try:
             cur.execute("delete from jincai")
             cur.execute("delete from banxue")
             cur.execute("delete from jincai_group")
             cur.execute("delete from banxue_group")
             cur.execute("delete from matches")
             self.conn.commit()
        except sqlite3.Error as e:
            print ("An error occurred: %s", e.args[0])
        finally:
            cur.close
            
    def createTable(self):
        print "create tables ..."
        C_SQL_Jincai = "create table if not exists jincai(_id integer primary key "\
            "autoincrement,no text,category text,name text,qty integer,price decimal(18,4)"\
            ",purchase text,purchasedate date ,usestate text,useway text,location text"\
            ",recodstatus int ,[CreatedTime] TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP )"
            #资产编号   资产分类    资产名称    数量  原值  取得方式    取得日期    使用状况    使用方向    存放（使用）地点    制单时间    实盘数量

        C_SQL_Banxue = "create table if not exists banxue(_id integer primary key "\
            "autoincrement,num text,name text,exname text,no text,category text,addway text"\
            ",purchase text,unit text,specification text,macno text,isasset text"\
            ",purchasedate date ,adddate date,subcode text,price decimal(18,4),amount decimal(18,4),qty integer"\
            ",recodstatus int ,[CreatedTime] TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP )"

        C_SQL_Jincai_Group = "create table if not exists jincai_group(_id integer primary key " \
            "autoincrement,name text,exname text,price decimal(18,4)"\
            ",purchasedate date ,qty integer,amount decimal(18,4),ids text"\
            ",recodstatus int ,[CreatedTime] TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP )"
     
        C_SQL_Banxue_Group  = "create table if not exists banxue_group(_id integer primary key "\
            "autoincrement,name text,exname text,price decimal(18,4)"\
            ",purchasedate date ,qty integer,amount decimal(18,4),ids text"\
            ",recodstatus int ,[CreatedTime] TimeStamp NOT NULL DEFAULT CURRENT_TIMESTAMP )"

        C_SQL_Result  = "create table if not exists matches(_id integer primary key "\
            "autoincrement,jc_id integer,bx_id integer)"
        
        cur = self.conn.cursor()
        try:
             cur.execute(C_SQL_Jincai)
             cur.execute(C_SQL_Banxue)
             cur.execute(C_SQL_Banxue_Group)
             cur.execute(C_SQL_Jincai_Group)
             cur.execute(C_SQL_Result)
             self.conn.commit()
        except sqlite3.Error as e:
            print ("An error occurred: %s", e.args[0])
        finally:
            cur.close


    def executemany(self,command,values):
        cur = self.conn.cursor()
        try:
            #for v in values:
            #    cur.execute(command,v)
            cur.executemany(command,values)
            self.conn.commit()

        except sqlite3.Error as e:
            print ("An error occurred: %s", e.args[0])
        finally:
            cur.close

    def execute(self,command,values):

        cur = self.conn.cursor()
        try:
            #for v in values:
            #    cur.execute(command,v)
            if len(values)==0:
                cur.execute(command)
            else:
                cur.execute(command,values)
            self.conn.commit()

        except sqlite3.Error as e:
            print ("An error occurred: %s", e.args[0])
        finally:
            cur.close

    def fetchall(self,command,values):
        cur = self.conn.cursor()
        try:
            #for v in values:
            #    cur.execute(command,v)
            if len(values)==0:
                cur.execute(command)
                return cur.fetchall()
            else:
                cur.execute(command,v)
                return cur.fetchall()
  
        except sqlite3.Error as e:
            print ("An error occurred: %s", e.args[0])
            return none
        finally:
            cur.close


    def readXls(self):
        print "read xls ..."
        book=xlrd.open_workbook(self.inputFile)
        #banxue=book.sheet_by_index(sheet_index_banxue)
        return book

    def loadJincai(self,book):
        print u"load jincai ..."
        #读取金财数据
        sheet=book.sheet_by_index(self.sheet_Index_Jincai)

        sqlcmd="insert into jincai(no,category,name,qty,price,purchase,purchasedate,usestate,useway,location,recodstatus) values(?,?,?,?,?,?,?,?,?,?,0)"
        #判断表格数据是否为空
        if sheet.nrows <= 0 or  sheet.ncols < 0:
            return
        #定义表格数组

        values=[]    
        for row in range(7, sheet.nrows):
            row_data = []
            #判断金财中的资产编号长度小于10则忽略此行数据
            if len(sheet.cell(row, 0).value)<=10:
                continue

            for col in range(sheet.ncols):
                #只读去9列数据
                if col>9:
                    continue

                data = sheet.cell(row, col).value
                row_data.append(data)

            #每500行数据写入一次数据库
            values.append(row_data)
            if len(values)>=1000:
                self.executemany(sqlcmd,values)
                values=[]

        self.executemany(sqlcmd,values)

    def loadBanxue(self,book):
        print u"load banxue ..."
        #读取金财数据
        sheet=book.sheet_by_index(self.sheet_Index_Banxue)

        sqlcmd="insert into banxue(num,name,exname,no,category,addway,purchase,unit,specification,macno,isasset"\
                ",purchasedate,adddate,subcode,price,amount,qty,recodstatus)"\
                " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)"

        #判断表格数据是否为空
        if sheet.nrows <= 0 or  sheet.ncols < 0:
            return
        #定义表格数组

        values=[]    
        for row in range(7, sheet.nrows):
            row_data = []
            #判断金财中的资产编号长度小于10则忽略此行数据
            if len(sheet.cell(row, 0).value)<=0:
                continue

            for col in range(sheet.ncols):
                #只读去9列数据
                if col>16:
                    continue
                if col==0:
                    data = int(sheet.cell(row, col).value)+10000000000
                else:
                    data = sheet.cell(row, col).value
                row_data.append(data)

            #每500行数据写入一次数据库
            values.append(row_data)
            if len(values)>=1000:
                self.executemany(sqlcmd,values)
                values=[]

        self.executemany(sqlcmd,values)


    def groupData(self):
        print "group data ..."
        sqlcmd = "insert into jincai_group(name,exname,price,purchasedate,amount,qty,ids,recodstatus)"\
            "select name,category exname,price,purchasedate,sum(price) amount,sum(qty) qty ,group_concat(no) ,0 from jincai group by category,name,price,purchasedate"
        self.execute(sqlcmd,[])
        sqlcmd = "insert into banxue_group(name,exname,price,purchasedate,amount,qty,ids,recodstatus)"\
            "select name,category exname,price,purchasedate,sum(amount) amount,sum(qty) qty ,group_concat(num),0 from banxue group by category,name,price,purchasedate"
        self.execute(sqlcmd,[])    

    def groupAnalyse(self):
        print "group analyse ..."
        #匹配所有购置年份相同并且单价相同的数据
        sqlcmd_step1="insert into matches(jc_id,bx_id)"\
                "select  a._id,b._id "\
                "from jincai_group a ,banxue_group b "\
                "where strftime(\"%Y\",a.purchasedate)=strftime(\"%Y\",b.purchasedate) and round(a.amount/a.qty,2)=b.price"

        #匹配所有购置年份相同并且金额相同的数据
        sqlcmd_step2="insert into matches(jc_id,bx_id)"\
                "select  a._id,b._id from jincai_group a ,banxue_group b "\
                "where strftime(\"%Y\",a.purchasedate)=strftime(\"%Y\",b.purchasedate) and a.amount=b.amount"
        self.execute(sqlcmd_step1,[])    
        self.execute(sqlcmd_step2,[])    


    def findBanxue(self,no):
        result=""
        if len(no)<=0:
            return result
        sqlcmd="select c.name,c.exname,c.price,c.purchasedate,c.ids from jincai_group  a left join matches b on a._id=b.jc_id left join banxue_group c on b.bx_id=c._id  where  instr( a.ids,'"+no+"')>0"
        r = self.fetchall(sqlcmd,[])
        if len(r) > 0 and r[0][0] != None:
            result= '资产名称:{0} 资产分类:{1}  原值:{2} 购置日期:{3}  资产编号:{4} '.format(r[0][0], r[0][1],r[0][2],r[0][3],r[0][4])
        return result

    def findJincai(self,no):
        result=""
        if len(no)<=0:
            return result
        no=int(no)+10000000000
        #print no
        sqlcmd="select c.name,c.exname,c.price,c.purchasedate, c.ids "\
            "from banxue_group  a left join matches b on a._id=b.bx_id left join jincai_group c on b.jc_id=c._id  where  instr( a.ids,'"+str(no).encode("gb2312")+"')>0"
        r = self.fetchall(sqlcmd,[])
        if len(r) > 0 and r[0][0] != None:
            result= '资产名称:{0} 扩展名:{1}  单价:{2} 购置日期:{3}  库序号:{4} '.format(r[0][0], r[0][1],r[0][2],r[0][3],r[0][4])
        return result
 
    def writeXls(self,book):
        print "write xls ..."
        #sheet=book
        #r_xls = xlrd.open_workbook(filename) 
        sheet_reader = book.sheet_by_index(5)
        w_xls = copy(book) 
        sheet_write = w_xls.get_sheet(5) 
        cols = sheet_reader.ncols 
        if sheet_reader.nrows > 0 and sheet_reader.ncols > 0:
            #for row in range(7, r_sheet.nrows):
            for row in range(7, sheet_reader.nrows ):    
                if len(sheet_reader.cell(row, 0).value)!=12:
                    continue
                banxue=self.findBanxue(sheet_reader.cell(row, 0).value)
                #print banxue.decode('utf-8')
                sheet_write.write(row, 15, banxue.decode('utf-8')) 

        sheet_reader = book.sheet_by_index(8)
        sheet_write = w_xls.get_sheet(8) 
  

        if sheet_reader.nrows > 0 and sheet_reader.ncols > 0:
            #for row in range(7, r_sheet.nrows):
            for row in range(7, sheet_reader.nrows ):    
                if len(sheet_reader.cell(row, 0).value)<=0:
                    continue
                jincai=self.findJincai(sheet_reader.cell(row, 0).value)
                #print banxue.decode('utf-8')
                sheet_write.write(row, 21, jincai.decode('utf-8')) 


        w_xls.save(self.outputFile)

def fileAnalyse(inFile,outFile):
    print inFile
    asset=schoolAsset(inFile,outFile)
    book = asset.readXls()
    asset.loadJincai(book)
    asset.loadBanxue(book)
    asset.groupData()
    asset.groupAnalyse()
    asset.writeXls(book)

def main(argv):
    inPath="./input"
    outPath="./out"
    files = os.listdir(inPath)
    fileList=[]
    for f in files:  
        if(os.path.isdir(inPath + '/' + f)):
            continue
        if(os.path.isfile(inPath + '/' + f)):
            fileList.append(f)  
    for f in fileList:
        i = f.find(".")
        if i>0 and f[i:].lower()==".xls":
            fileAnalyse(inPath + '/' + f,outPath + '/' + f)


if __name__=='__main__':
    main(sys.argv)
