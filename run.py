from PyQt5 import QtWidgets, uic
from PyQt5 import QtCore
import sys
import pandas as pd
import numpy as np 
import pyodbc  as odbc
import datetime 
from sqlalchemy import create_engine
import sqlite3 as s
app=QtWidgets.QApplication([])
item=QtWidgets.QTableWidgetItem
call=uic.loadUi("front.ui")
call=uic.loadUi("rw.ui")
########
str1='actuarialdb'
str2='actuarialdb'
str3='actuarialdb'
str4='actuarialdb'
str5='dbo.[super actsubdb_miu_2020Q1_bookings_final]'
str6='dbo.[MIU_SISAD_ITC_202001]'
str7=''
strfil = ''
connectionstr2 ='a'
connectionstr1 ='b'
obu_names = "'International - Casualty Primary'"
d = 1
file_level = 1
recon_type='ACS'
save_file_path = r'C:\Users\x116135\Desktop\SISAD Q3 ITC\Losses'

#########
connt = odbc.connect('Driver={SQL Server};'
                      'Server=actuarialdb;'
                      'Database=actuarialdb;'
                      'Trusted_Connection=yes;')
call.typeofrecons.clear()
recontypeqry="SELECT Distinct [Type_Of_recon] as Type_Of_recon  FROM dbo.rt_t1100_DataReconMappings"
df_rec_type= pd.read_sql(recontypeqry, connt)
df_rec_type.reset_index(drop=True)
call.typeofrecons.addItems(df_rec_type['Type_Of_recon'])

nameqry="select Top 1 Name from dbo.actuarial_users where 'R02\\'+[emp id]=user"
username=pd.read_sql(nameqry, connt)
username.reset_index(drop=True,inplace=True)
username =username.iat[0,0]
username="Welcome: "+username
call.label_15.setText(username)

def withoutediting():
    global recon_type
    recon_type=call.typeofrecons.currentText()
    print(recon_type)

def withouteditingtable():
    global call,app,connt
    global df  
    app.exec_()
    # app=QtWidgets.QApplication([])
    call=uic.loadUi("rw.ui")
    call.typeofrecons.clear()
    recontypeqry="SELECT Distinct [Type_Of_recon] as Type_Of_recon  FROM dbo.rt_t1100_DataReconMappings"
    df_rec_type= pd.read_sql(recontypeqry, connt)
    df_rec_type.reset_index(drop=True)
    call.typeofrecons.addItems(df_rec_type['Type_Of_recon'])

    nameqry="select Top 1 Name from dbo.actuarial_users where 'R02\\'+[emp id]=user"
    username=pd.read_sql(nameqry, connt)
    username.reset_index(drop=True,inplace=True)
    username =username.iat[0,0]
    username="Welcome: "+username
    call.label_15.setText(username)


    call.show()
    app.exec()
    call.pushButton2.clicked.connect(clearcombo)
    call.saveserver.clicked.connect(getdb)
    call.savedb.clicked.connect(gettables)
    call.savetable.clicked.connect(getall)
    call.pushButton.clicked.connect(runAll)
    call.addfilter.clicked.connect(filteraddition)
    call.addmeasure.clicked.connect(filteradditionmeasure)
    call.confirmfilter.clicked.connect(filterfinal)
    call.confirmmeasure.clicked.connect(filterfinalmeasure)
    call.confirmfilter0.clicked.connect(setfiltercolumn)
    call.editmappings.clicked.connect(table_ui)
    call.woediting.clicked.connect(withoutediting)

#######
def table_ui():
    global call,app
    global recon_type,horHeaders,df
    conn = odbc.connect('Driver={SQL Server};'
                      'Server=actuarialdb;'
                      'Database=actuarialdb;'
                      'Trusted_Connection=yes;')
    
    recon_type=call.typeofrecons.currentText()
    print(recon_type)
    Query = "select [SNo], [List of Columns],[List of Columns1],[Measure/Dimension],[Filter],[NULL Check],[Distinct Check],[Granularity],[Priority],"
    Query = Query+"[Length Check],[Datatype Check],[sign_convention],[type_of_measure] from [dbo].[rt_t1100_DataReconMappings] where [Type_Of_recon] = '"+recon_type+"'"
    df1= pd.read_sql(Query, conn)
    t1=df1.iat[0,0]
    df=df1.copy()
    df.fillna('',inplace=True)

    app.exec_()
    # app=QtWidgets.QApplication([])
    call=uic.loadUi("front.ui")
    call.show()
    app.exec()

    call.Table1.setRowCount(df.shape[0])
    call.Table1.setColumnCount(df.shape[1])
    horHeaders=df1.columns
    call.Table1.setHorizontalHeaderLabels(horHeaders)
    call.Table1.setHorizontalHeaderLabels(horHeaders)

    for row in range (0, df.shape[0]):
        for col in range (0, df.shape[1]):
            data = str(df.iat[row, col] )
            call.Table1.setItem(row, col, item(str(data)))

    call.pushButton.clicked.connect(rw_ui)
    call.woeditingTable.clicked.connect(withouteditingtable)
    call.rowadd.clicked.connect(addrow)
    # app.exec_()
    # app=QtWidgets.QApplication([])
    # call=uic.loadUi("rw.ui")
    # call.show()
    # app.exec()


def getdb():
    global str1
    global str2
    
    str1=call.s1.currentText()
    connectionstr1='Driver={SQL Server}; Server='+str1.strip()+'; Database=master; Trusted_Connection=yes;'
    connection1 = odbc.connect(connectionstr1)
    sqldb="SELECT DB_NAME(database_id) AS DatabaseName FROM sys.databases"
    df1= pd.read_sql(sqldb, connection1)
    df1.reset_index(drop=True)
    call.db1.clear()
    call.db1.addItems(df1['DatabaseName'])
    str2=call.s2.currentText()
    connectionstr2='Driver={SQL Server}; Server='+str2.strip()+'; Database=master; Trusted_Connection=yes;'
    connection2 = odbc.connect(connectionstr2)
    sqldb="SELECT DB_NAME(database_id) AS DatabaseName FROM sys.databases"
    df2= pd.read_sql(sqldb, connection2)
    df2.reset_index(drop=True)
    call.db2.clear()
    call.db2.addItems(df2['DatabaseName'])

def gettables():   
    global str3
    global str4
    global connectionstr1
    global connectionstr2
    call.t1.clear()
    call.t2.clear()
    str3=call.db1.currentText()
    dbqry1="SELECT TABLE_SCHEMA+'.['+TABLE_NAME+']' as TableName FROM INFORMATION_SCHEMA.TABLES where (TABLE_NAME like '%super%actsubdb_miu%' or TABLE_NAME like '%tisad%') order by 1"
    connectionstr1='Driver={SQL Server}; Server='+str1+'; Database='+str3+'; Trusted_Connection=yes;'
    connection1 = odbc.connect(connectionstr1)    
    dff1= pd.read_sql(dbqry1, connection1)
    dff1.reset_index(drop=True)
    call.t1.addItems(dff1['TableName'])
    str4=call.db2.currentText()
    dbqry2="SELECT TABLE_SCHEMA+'.['+TABLE_NAME+']' as TableName FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME like '%sisad%' order by 1"
    connectionstr2='Driver={SQL Server}; Server='+str2+'; Database='+str4+'; Trusted_Connection=yes;'
    connection2 = odbc.connect(connectionstr2)    
    dff2= pd.read_sql(dbqry2, connection2)
    dff2.reset_index(drop=True)
    call.t2.addItems(dff2['TableName'])

def getall():   
    global str5
    global str6
    global str7
    global obu_names
    global df1,recon_type
    str5=call.t1.currentText()
    str6=call.t2.currentText()
    call.filter_down.clear()
    # dbqry5="SELECT distinct [OperatingUnit/OBU] as obu FROM "+str6    
    # connection2 = odbc.connect(connectionstr2)    
    # obudf= pd.read_sql(dbqry5, connection2)
    # df0.reset_index(drop=True)
    conn = odbc.connect('Driver={SQL Server};'
                      'Server=actuarialdb;'
                      'Database=actuarialdb;'
                      'Trusted_Connection=yes;')
    Query = "select [SNo], [List of Columns],[List of Columns1],[Measure/Dimension],[Filter],[NULL Check],[Distinct Check],[Granularity],[Priority],"
    Query = Query+"[Length Check],[Datatype Check],[sign_convention],[type_of_measure] from [dbo].[rt_t1100_DataReconMappings] where [Type_Of_recon] = '"+recon_type+"'"
    df1= pd.read_sql(Query, conn)
    print(df1)
    
    call.filter_down.addItems(df1['List of Columns'])
    # call.obuset.setText("")
    str7=""
    # print(str5)
    # print(str6)

def setfiltercolumn():
    global df1,connectionstr1,connectionstr2,d,file_level,recon_type,connt,flist
    connection1=odbc.connect(connectionstr1)
    connection2=odbc.connect(connectionstr2)
    strs=call.filter_down.currentText()
    call.filterbox.clear()
    print(df1)
    d=df1[df1['List of Columns']==strs].SNo.item()
    Sr_filelevel = d
    file_level = Sr_filelevel
    d1=df1[df1['SNo']==d]['List of Columns'].item()
    d2=df1[df1['SNo']==d]['List of Columns1'].item()
    filterqry1="SELECT distinct "+d1+"as a from "+str5
    filterqry2="SELECT distinct "+d2+"as a from "+str6
    f1=pd.read_sql(filterqry1, connection1)
    f2=pd.read_sql(filterqry2, connection2)
    flist = pd.concat([f1, f2], axis=0, sort=False)
    flist.drop_duplicates(inplace=True)
    flist.reset_index(drop=True,inplace=True)
    filtermeasure1 = "SELECT distinct type_of_measure as b from dbo.[rt_t1100_DataReconMappings] ct WHERE Type_of_Recon = '%s' and type_of_measure is not  null" %(recon_type)
    f3 = pd.read_sql(filtermeasure1, connt)
    f3.reset_index(drop=True,inplace=True)
    print(f3)
    call.filtermeasure.clear()
    call.filterbox.addItems(['All'])
    call.filterbox.addItems(flist['a'])
    print('about to fill measure')
    call.filtermeasure.addItems(f3['b'])

def filteraddition():
    global str7
    str7=str7+"'"+call.filterbox.currentText()+"',"
    call.filterf.setText(str7)

def filteradditionmeasure():
    global strfil
    strfil=strfil+"'"+call.filtermeasure.currentText()+"',"
    call.checkboxmeasure.setText(strfil)

def filterfinal():
    global obu_names,flist
    obu_names=str7[:-1]

    if obu_names == "'All'":
        flist['a'] = "'" + flist['a'] + "'"
        strflist = ",".join(flist['a'])
        obu_names=strflist[:]
        print(obu_names)
    elif obu_names != "'All'":
        obu_names=''+obu_names+''
    
    obu_names=obu_names.replace("'","''")
    print(obu_names)

def filterfinalmeasure():
    global measure_names,strfil
    measure_names=strfil[:-1]
    measure_names=''+measure_names+''
    # measure_names=measure_names.replace("'","''")
    print(measure_names)

def clearcombo():
    call.db1.clear()
    call.db2.clear()
    call.t1.clear()
    call.t2.clear()
    call.filterbox.clear()

def dtcheck():
    global df0
    #mapping table to be fetched from Connection 1
    # connectionstr1
    connection1 = odbc.connect(connectionstr1)    
    mapping_query = "select * from [dbo].[rt_t1100_DataReconMappings] where "
    df0= pd.read_sql(mapping_query, connection1)
    df1=df0[df0['Distinct Check']==1]
    columns1 = df1['List of Columns'].copy()
    columns2 = df1['List of Columns1'].copy()
    columns1=columns1.str.replace("[","'")
    columns1=columns1.str.replace("]","'")
    columns2=columns2.str.replace("[","'")
    columns2=columns2.str.replace("]","'")
    column1 = columns1.str.cat(sep=',')
    column2 = columns2.str.cat(sep=',')
    column1="("+column1+")"
    column2="("+column2+")"

    # s1=call.server1.toPlainText()
    # s2=call.server2.toPlainText()
    # d1=call.database1.toPlainText()
    # d2=call.database2.toPlainText()
    # t1=call.tablename1.toPlainText()
    # t2=call.tablename2.toPlainText()
    # connectionstr1='Driver={SQL Server}; Server='+s1.strip()+'; Database='+d1.strip()+'; Trusted_Connection=yes;'
    # connectionstr2='Driver={SQL Server}; Server='+s2.strip()+'; Database='+d2.strip()+'; Trusted_Connection=yes;'
    connection1 = odbc.connect(connectionstr1)
    connection2 = odbc.connect(connectionstr2)


    check1="SELECT distinct upper(c.name) 'Column Name', t.Name 'Data type', c.max_length 'Max Length' "
    check1=check1+"FROM sys.columns c "
    check1=check1+"INNER JOIN "
    check1=check1+"    sys.types t ON c.user_type_id = t.user_type_id "
    check1=check1+"LEFT OUTER JOIN "
    check1=check1+"    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
    check1=check1+" LEFT OUTER JOIN "
    check1=check1+"    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
    check1=check1+" WHERE "
    check1=check1+"    c.object_id = OBJECT_ID('"+str5+"')"
    check1=check1+"and c.name in %s" %(column1)

    check2="SELECT distinct upper(c.name) 'Column Name', t.Name 'Data type', c.max_length 'Max Length' "
    check2=check2+"FROM sys.columns c "
    check2=check2+"INNER JOIN "
    check2=check2+"    sys.types t ON c.user_type_id = t.user_type_id "
    check2=check2+"LEFT OUTER JOIN "
    check2=check2+"    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
    check2=check2+" LEFT OUTER JOIN "
    check2=check2+"    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
    check2=check2+" WHERE "
    check2=check2+"    c.object_id = OBJECT_ID('"+str6+"')"
    check2=check2+"and c.name in %s" %(column2)
   
    datatype1= pd.read_sql(check1, connection1)
    datatype2= pd.read_sql(check2, connection2)

    dfjoin=datatype1.merge(datatype2,how='outer' ,left_on='Column Name', right_on='Column Name')
    # dfjoin = pd.merge(datatype1,datatype2,on=['Column Name'])
    dfjoin['Datatype Check'] = 'other'
    dfjoin.loc[dfjoin['Data type_x'] == dfjoin['Data type_y'], 'Datatype Check'] = 'match'
    dfjoin.loc[dfjoin['Data type_x'] != dfjoin['Data type_y'], 'Datatype Check'] = 'mismatch'

    dfjoin['Datatype Length Check'] = 'other'
    dfjoin.loc[dfjoin['Max Length_x'] == dfjoin['Max Length_y'], 'Datatype Length Check'] = 'match'
    dfjoin.loc[dfjoin['Max Length_x'] != dfjoin['Max Length_y'], 'Datatype Length Check'] = 'mismatch'

    dfjoin.rename(columns={"Max Length_x": "Max Length table1", "Max Length_y": "Max Length table2"},inplace=True)
    dfjoin.rename(columns={"Data type_x": "Data type table1", "Data type_y": "Data type table2"},inplace=True)

    dfjoin_final=dfjoin.copy()
    dfjoin_final=dfjoin_final.fillna('NA')
    filter1 = dfjoin_final['Datatype Check']=='mismatch'
    filter2 = dfjoin_final['Datatype Length Check']=='mismatch'
    # print(filter1)
    # filter1= filter1.fillna('dummy')
    # print(filter2)
    # filter2= filter1.fillna('dummy')
    dfjoin_final.where(filter1 | filter2, inplace = True)
    dfjoin_final.dropna(inplace=True)
    # print(dfjoin)
    print(dfjoin_final)

    f_path = '%s\dfjoin_final.xlsx' %(save_file_path)
    dfjoin_final.to_excel(f_path,sheet_name = "Datatype_Check",index=False) 
    # dfjoin.to_excel(r'C:\Users\X133989\Desktop\python\dfjoin.xlsx',sheet_name = "Datatype_Check",index=False) 
    print("Datatype Check is Complete.")

def DebugDifferences(GrainChecks_qry_Res,priorities,priorities1,measures_2):
    data = pd.DataFrame([])
    data3= pd.DataFrame([])
    for i in range(len(priorities)):
        df = priorities['List of Columns'].iloc[i]
        grains = [df]
        grains = pd.Series(grains).append(measures_2)
        grains = grains.apply(lambda x: x[1:len(x)-1])
        df_debug = GrainChecks_qry_Res[grains]
        df_debug = df_debug.groupby(df[1:len(df)-1]).sum()
        df_debug = df_debug.round(0)
        data2 = pd.DataFrame(columns = ['filter','column'])
        data4 = pd.DataFrame(columns = ['filter'])
        for index, row in df_debug.iterrows():
            df1 = row.abs()
            check = df1.sum()
            if check != 0:
                data1 = pd.DataFrame(columns = ['filter'])
                dat = pd.DataFrame(columns = ['filter'])
                index1 = "'"  + index + "'"
                data1['filter'] = [index1]
                dat['filter'] = [index]
                data4 = data4.append(dat)
                data2 = data2.append(data1)
                
        if data2['filter'].empty:
            data['filter'] = [str(-999)]
        else:
            data['filter'] = [str((data2['filter']).str.cat(sep=','))]
        data['column'] = [df]
        data['column1'] = [priorities1['List of Columns1'].iloc[i]]
        GrainChecks_qry_Res = GrainChecks_qry_Res[GrainChecks_qry_Res[df[1:len(df)-1]].isin(data4['filter'])]
        data3 = data3.append(data)
    return data3

def GetPriorities(dimensions,measures):
    priorities = measures.loc[measures['Priority'].notnull(),['type_of_measure','Priority']]
    priorities = priorities.groupby('type_of_measure').head(1).reset_index(drop=True)
    priorities_columns = pd.DataFrame([])
    for index, row in priorities.iterrows():
        S1 = row['Priority'] 
        S1 = S1.split(",") 
        S1 = [int(i) for i in S1] 
        df=dimensions.set_index('SNo').loc[S1].reset_index()
        priorities_columns1 = pd.DataFrame([])
        priorities_columns1['List of Columns'] = df['List of Columns']
        priorities_columns1['List of Columns1'] = df['List of Columns1']
        priorities_columns1['type_of_measure'] = row['type_of_measure']
        priorities_columns = priorities_columns.append(priorities_columns1)
    return priorities_columns

def debugCheck():
    global str5,str6,obu_names,file_level,d
    conn = odbc.connect(connectionstr1)    
    conn1 = odbc.connect(connectionstr2)
    Start_Time = datetime.datetime.now()

    Query = "Update ct set ct.Filter = 'IN ( %s )' FROM dbo.[rt_t1100_DataReconMappings] ct WHERE SNo = '%s' and Type_of_Recon = '%s' and isnull(type_of_measure,'') in ('',%s)" % (obu_names, file_level,recon_type,measure_names)
    cursor = conn.cursor()
    cursor.execute(Query)

    Query = "select * from [dbo].[rt_t1100_DataReconMappings] where Type_of_Recon = '%s' and isnull(type_of_measure,'') in ('',%s)" %(recon_type,measure_names)

    df1= pd.read_sql(Query, conn)

    dimensions = df1[df1['Measure/Dimension']=='D']
    measures = df1[df1['Measure/Dimension']=='M']
    File_Name_Level_1 = dimensions.loc[dimensions['SNo']== file_level,'List of Columns']
    File_Name_Level_2 = dimensions.loc[dimensions['SNo']== file_level,'List of Columns1']

    column1 = File_Name_Level_1.apply(lambda x: x[1:len(x)-1])
    column2 = File_Name_Level_2.apply(lambda x: x[1:len(x)-1])

    #getpropritylist for Premiums
    priorities_columns = GetPriorities(dimensions,measures)

    type_of_measures = pd.Series(measures['type_of_measure']).unique()

    for k in range(len(type_of_measures)):
        priorities = priorities_columns.loc[priorities_columns['type_of_measure'] == type_of_measures[k],['List of Columns']]
        priorities1 = priorities_columns.loc[priorities_columns['type_of_measure'] == type_of_measures[k],['List of Columns1']]
        
       
        #CREATING FILTERS FOR TABLE 1
        dimensions_1 = priorities_columns.loc[priorities_columns['type_of_measure'] == type_of_measures[k],['List of Columns']]
        dimensions_1 = pd.Series(dimensions_1.iloc[:,0])
        measures_1 = measures['List of Columns']
        #CREATING ISNULL(FILTER COLUMNS)
        dimensions_filter = dimensions.copy()
        dimensions_filter['List of Columns'] = "isnull(" + dimensions_filter['List of Columns'] + ",'')"
        filters_1 = dimensions_filter.loc[dimensions_filter['Filter'].notnull(),['List of Columns','Filter']]
        filters_1 = filters_1.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')

        #CREATING FILTERS FOR TABLE 2
        dimensions_2 = priorities_columns.loc[priorities_columns['type_of_measure'] == type_of_measures[k],['List of Columns1']]
        dimensions_2 = pd.Series(dimensions_2.iloc[:,0])
        measures_2 = measures['List of Columns1']

        #CREATING ISNULL(FILTER COLUMNS)
        dimensions_filter['List of Columns1'] = "isnull(" + dimensions_filter['List of Columns1'] + ",'')"
        filters_2 = dimensions_filter.loc[dimensions_filter['Filter'].notnull(),['List of Columns1','Filter']]
        filters_2 = filters_2.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')
        #CREATING SELECT GRAINS FROM TABLE 1
        Grains_Select1 = "upper(ltrim(rtrim(" + dimensions_1  + "))) "  + dimensions_1
        Grains_Select1 = Grains_Select1.str.cat(sep=',') 
        
        #CREATING SELECT GRAINS FROM TABLE 2
        Grains_Select2 = "upper(ltrim(rtrim(" + dimensions_2 + "))) "  + dimensions_1
        Grains_Select2 = Grains_Select2.str.cat(sep=',') 

        #CREATING GROUP BY GRAINS FROM TABLE 1
        dimensions_1 = "upper(ltrim(rtrim(" + dimensions_1  + "))) "
        Grains_1 = dimensions_1.str.cat(sep=',') 
    
        #CREATING GROUP BY GRAINS FROM TABLE 2
        dimensions_2 = "upper(ltrim(rtrim(" + dimensions_2  + "))) "
        Grains_2 = dimensions_2.str.cat(sep=',') 

        measures_G = measures[measures['type_of_measure']==type_of_measures[k]]
        #CREATING PREMIUM MEASURES 
        measures_1 = measures_G['List of Columns']
        measures_2 = measures_G['List of Columns1']

        #TABLE1
        measures_grain_check_1 = "Sum(" + measures_1 + ") " + measures_2
        measures_grain_check_1 = measures_grain_check_1.str.cat(sep=',') 

        #TABLE2
        measures_grain_check_2 = measures_G['sign_convention'] + "*-1*Sum(" + measures_G['List of Columns1'] + ")" + " " + + measures_2
        measures_grain_check_2 = measures_grain_check_2.str.cat(sep=',')

        #HAVING CONDITION
        measures_grain_check_H = " Sum(" + measures_1 + ") <> 0 " 
        measures_grain_check_H = measures_grain_check_H.str.cat(sep='OR') 
        
        measures_grain_check_H_2 = " Sum(" + measures_2 + ") <> 0 " 
        measures_grain_check_H_2 = measures_grain_check_H_2.str.cat(sep='OR') 
    
        GrainChecks_qry1 = """SELECT 'Actuarialdb' source,  %s ,
                    %s 
                    FROM  %s c 
                    WHERE (%s )
                    GROUP BY %s
                    HAVING (%s)
                    """ % (Grains_Select1, measures_grain_check_1,str5,filters_1, Grains_1,measures_grain_check_H)
            
        GrainChecks_qry2 = """SELECT  'SISAD' source , %s ,
                        %s 
                        FROM  %s
                        WHERE ( %s )
                        GROUP BY %s
                        HAVING (%s)
                    """ % (Grains_Select2, measures_grain_check_2,str6,filters_2, Grains_2,measures_grain_check_H_2)
        GrainChecks_qry_Res1 = pd.read_sql(GrainChecks_qry1, conn)
        GrainChecks_qry_Res2 = pd.read_sql(GrainChecks_qry2, conn1)
        GrainChecks_qry_Res=  pd.concat([GrainChecks_qry_Res1, GrainChecks_qry_Res2])

        d= pd.Series((GrainChecks_qry_Res[File_Name_Level_1.apply(lambda x: x[1:len(x)-1])].iloc[:,0])).unique()
       
        OBU = ''.join(e for e in obu_names[1:len(obu_names)-1] if e.isalnum())
        
        from pandas import ExcelWriter
        from openpyxl import load_workbook
        

        for i in range(len(d)):
            if k == 0:
                FilePath = r"%s\Debug\%s.xlsx" %(save_file_path,'D-'+d[i]) 
                writer = pd.ExcelWriter(FilePath)
                print("Created new file")
            else:
                print("Reopened file")
                FilePath = r"%s\Debug\%s.xlsx" %(save_file_path,'D-'+d[i]) 
                book = load_workbook(FilePath)
                writer = pd.ExcelWriter(FilePath, engine='openpyxl') 
                writer.book = book
                writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

            e=GrainChecks_qry_Res[column1] == d[i]
            es = pd.Series(e.iloc[:,0])
            Debug_check = GrainChecks_qry_Res[es]
            sheetname = r'%s Check on Debug Fields'%(type_of_measures[k])
            Debug_check.to_excel(writer, sheet_name=sheetname)
                
            comment = "Debugging Recons for %s for:"%(type_of_measures[k])
            print(comment)
            print(d[i])

            data = DebugDifferences(Debug_check,priorities,priorities1,measures_2)
            sheetname = '%s Debug Fields'%(type_of_measures[k])

            data.to_excel(writer, sheet_name=sheetname)
            indexNames = data[data['filter'] == '-999'].index      
            data.drop(indexNames , inplace = True)

            if data.empty==0:
                data1 = "isnull(" + data['column'] +  ",'')" + " in ( " + data['filter'] + ")"
                FilterstoDebug = data1.str.cat(sep=' AND ')
                data12= "isnull(" + data['column1'] +  ",'')" + " in ( " + data['filter'] + ")"
                FilterstoDebug1 = data12.str.cat(sep=' AND ')
                
                dimensions_filter = dimensions[~dimensions['List of Columns'].isin(File_Name_Level_1)]
                dimensions_filter['List of Columns'] = "isnull(" + dimensions_filter['List of Columns'] + ",'')"
                filters_1_D = dimensions_filter.loc[dimensions_filter['Filter'].notnull(),['List of Columns','Filter']]
                if filters_1_D.empty==0:
                    filters_1_D = filters_1_D.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')
                    filters_1_debug = filters_1_D + " AND " + FilterstoDebug
                else:
                    filters_1_debug = FilterstoDebug

                dimensions_filter['List of Columns1'] = "isnull(" + dimensions_filter['List of Columns1'] + ",'')"
                filters_2_D = dimensions_filter.loc[dimensions_filter['Filter'].notnull(),['List of Columns1','Filter']]

                if filters_2_D.empty==0:
                    filters_2_D = filters_2_D.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')
                    filters_2_debug = filters_2_D + " AND " + FilterstoDebug1
                else:
                    filters_2_debug = FilterstoDebug1


                GrainChecks_qry1 = """SELECT 'Actuarialdb' source,%s ,
                            %s 
                            FROM  %s c 
                            WHERE ( %s )
                            GROUP BY %s
                            """ % (Grains_Select1, measures_grain_check_1,str5,filters_1_debug, Grains_1)
                GrainChecks_qry2 = """SELECT 'SiSAD' source, %s ,
                                %s 
                                FROM  %s
                                WHERE ( %s )
                            GROUP BY %s
                        """ % (Grains_Select2, measures_grain_check_2,str6,filters_2_debug, Grains_2)  
        
                GrainChecks_qry_Res1 = pd.read_sql(GrainChecks_qry1, conn)
                GrainChecks_qry_Res2 = pd.read_sql(GrainChecks_qry2, conn1)

                if len(GrainChecks_qry_Res1) == 0:
                    GrainChecks_qry_Res_D = GrainChecks_qry_Res2
                if len(GrainChecks_qry_Res2) == 0:
                    GrainChecks_qry_Res_D = GrainChecks_qry_Res1
                else:
                    GrainChecks_qry_Res_D=  pd.concat([GrainChecks_qry_Res1, GrainChecks_qry_Res2])
            else:
                GrainChecks_qry_Res_D = pd.DataFrame([])
            sheetname = "%s Debugged"%(type_of_measures[k])
            GrainChecks_qry_Res_D.to_excel(writer, sheet_name=sheetname)
            writer.save()
            print(FilePath)
            print("Saving the file")
    End_Time = datetime.datetime.now()
    print("Debug Check is complete in: ") 
    print(End_Time-Start_Time)
    print("Debugging is complete.")  

def nullcheck():

    conn = odbc.connect(connectionstr1)  
    conn1 = odbc.connect(connectionstr2)      
    Query = "SELECT * FROM [dbo].[rt_t1100_DataReconMappings]"                       
    df1= pd.read_sql(Query, conn)
    dimension_1 = df1[df1['NULL Check']==1]
    dimension_2 = dimension_1['List of Columns']
    dimension_3 = dimension_1['List of Columns1']
    writer = pd.ExcelWriter(r'%s\CheckNull.xlsx') %(save_file_path)

    for i in range(len(dimension_2)) : 
        qry_1 = """SELECT *
            FROM  (Select top 100 a.*, b.Bu_code from dbo.[super actsubdb_miu_2020Q1_bookings] a
            left join sparta.dbo.rt_t0025_mas_le b
                on a.[MIU Carco] = b.[MIU Carco]) c 
            WHERE %s IS NULL
                """ % (dimension_2[i])
        qry_2 = """SELECT count(*)
            FROM  (Select top 100 a.*, b.Bu_code from dbo.[super actsubdb_miu_2020Q1_bookings] a
            left join sparta.dbo.rt_t0025_mas_le b
                on a.[MIU Carco] = b.[MIU Carco]) c 
            WHERE %s IS NULL
                """ % (dimension_2[i])  
        GrainChecks_qry_Res_1= pd.read_sql(qry_2, conn).all()              
        if(GrainChecks_qry_Res_1.all()!=0) :
            GrainChecks_qry_Res= pd.read_sql(qry_1, conn)
            sheet_name = "A-" + ''.join(e for e in dimension_2[i][1:len(dimension_2[i])-1] if e.isalnum())
            GrainChecks_qry_Res.to_excel(writer, sheet_name=sheet_name)

    for j in range(len(dimension_3)) : 
        qry_2 = """ Select top 100 a.* from dbo.[MIU_SISAD_ITC_202001] a
            WHERE %s IS NULL
                """ % (dimension_3[j])
        qry_3 = """ Select count(*) from dbo.[MIU_SISAD_ITC_202001] 
            WHERE %s IS NULL
                """ % (dimension_3[j])                      
        GrainChecks_qry_Res_3= pd.read_sql(qry_3, conn1).all()
        if(GrainChecks_qry_Res_3.all()!=0) :
            GrainChecks_qry_Res= pd.read_sql(qry_2, conn1)    
            sheet_name = "B-" + ''.join(e for e in dimension_3[j][1:len(dimension_3[j])-1] if e.isalnum())
            GrainChecks_qry_Res.to_excel(writer, sheet_name=sheet_name) 
        else :
            print('no')

    writer.save()  
    print("Null Check is complete.") 

def distinctCheck():
    conn = odbc.connect(connectionstr1)
    conn1 = odbc.connect(connectionstr2)
    Query = "select * from [dbo].[rt_t1100_DataReconMappings]"
    df1= pd.read_sql(Query, conn)

    dimensions = df1[df1['Measure/Dimension']=='D']
    measures = df1[df1['Measure/Dimension']=='M']

    #CREATING FILTERS FOR TABLE 1
    dimensions_1 = dimensions.loc[dimensions['Granularity']==1,'List of Columns']
    measures_1 = measures['List of Columns']
    #CREATING ISNULL(FILTER COLUMNS)
    dimensions_filter = dimensions.copy()
    dimensions_filter['List of Columns'] = "isnull(" + dimensions_filter['List of Columns'] + ",'')"
    filters_1 = dimensions_filter.loc[dimensions_filter['Filter'].notnull(),['List of Columns','Filter']]
    filters_1 = filters_1.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')

    #CREATING FILTERS FOR TABLE 2
    dimensions_2 = dimensions.loc[dimensions['Granularity']==1,'List of Columns1']
    measures_2 = measures['List of Columns1']
    #CREATING ISNULL(FILTER COLUMNS)
    dimensions_filter['List of Columns1'] = "isnull(" + dimensions_filter['List of Columns1'] + ",'')"
    filters_2 = dimensions_filter.loc[dimensions_filter['Filter'].notnull(),['List of Columns1','Filter']]
    filters_2 = filters_2.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')

    #CREATING GROUP BY GRAINS FROM TABLE 1
    Grains_1 = dimensions_1.str.cat(sep=',') 
    #CREATING GROUP BY GRAINS FROM TABLE 2
    Grains_2 = dimensions_2.str.cat(sep=',') 

    """
    Distinct check. Get grains to check distinct on. Run Except Query.
    """

    Distinct_Grains_1 = df1.loc[df1['Distinct Check']==1,'List of Columns'].str.cat(sep=',')
    Distinct_Grains_2 = df1.loc[df1['Distinct Check']==1,'List of Columns1'].str.cat(sep=',')

    Distinct_Check_Qry_1 = """SELECT %s 
                            FROM  (Select a.*, b.Bu_code from dbo.[super actsubdb_miu_2020Q1_bookings] a
                            left join sparta.dbo.rt_t0025_mas_le b
                            on a.[MIU Carco] = b.[MIU Carco]) c 
                            WHERE %s 
                            Except
                            SELECT %s 
                            FROM  dbo.[MIU_SISAD_ITC_202001] 
                            WHERE %s""" % (Distinct_Grains_1,filters_1,Distinct_Grains_2,filters_2)


    Distinct_Check_Qry_2 = """SELECT %s 
                            FROM  dbo.[MIU_SISAD_ITC_202001] 
                            WHERE %s
                            EXCEPT
                            SELECT %s 
                            FROM  (Select a.*, b.Bu_code from dbo.[super actsubdb_miu_2020Q1_bookings] a
                            left join sparta.dbo.rt_t0025_mas_le b
                            on a.[MIU Carco] = b.[MIU Carco]) c 
                            WHERE %s """ % (Distinct_Grains_2,filters_2,Distinct_Grains_1,filters_1)

    Distinct_Check_Qry_1_Res= pd.read_sql(Distinct_Check_Qry_1, conn)
    Distinct_Check_Qry_2_Res= pd.read_sql(Distinct_Check_Qry_2, conn1)
    f_path = r'%s\DistinctCheck.xlsx' %(save_file_path)
    with pd.ExcelWriter(f_path) as writer:  
        Distinct_Check_Qry_1_Res.to_excel(writer, sheet_name='SubDB vs SISAD')
        Distinct_Check_Qry_2_Res.to_excel(writer, sheet_name='SISAD vs SubDB')
    writer.save()
    print("Distinct Check is complete.") 

def granularCheck():
    global str5
    global str6 ,obu_names,file_level,d
    conn = odbc.connect(connectionstr1)    
    conn1 = odbc.connect(connectionstr2)  
    # obu_names = "''SP - ASB Art''" 
    Start_Time = datetime.datetime.now()

    Query = "Update ct set ct.Filter = 'IN ( %s )' FROM dbo.[rt_t1100_DataReconMappings] ct WHERE SNo = '%s' and Type_of_Recon  = '%s' and isnull(type_of_measure,'') in ('',%s)" % (obu_names,file_level,recon_type,measure_names)
    cursor = conn.cursor()
    cursor.execute(Query)
    print(Query)

    Query = "select * from dbo.[rt_t1100_DataReconMappings] WHERE Type_of_Recon  = '%s' and isnull(type_of_measure,'') in ('',%s)" %(recon_type,measure_names)
    df1= pd.read_sql(Query, conn)

    dimensions = df1[df1['Measure/Dimension']=='D']
    measures = df1[df1['Measure/Dimension']=='M']


    #CREATING FILTERS FOR TABLE 1
    
    dimensions_D = dimensions['List of Columns']
    dimensions_1 = dimensions.loc[dimensions['Granularity']==1,'List of Columns']
    measures_1 = measures['List of Columns']
    
    #CREATING ISNULL(FILTER COLUMNS)
    dimensions_filter_T = dimensions.copy()
    dimensions_filter_T['List of Columns'] = "isnull(" + dimensions_filter_T['List of Columns'] + ",'')"
    filters_1_T = dimensions_filter_T.loc[dimensions_filter_T['Filter'].notnull(),['List of Columns','Filter']]
    filters_1_T = filters_1_T.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')

    #CREATING FILTERS FOR TABLE 2
    dimensions_D_2 = dimensions['List of Columns1']
    dimensions_2 = dimensions.loc[dimensions['Granularity']==1,'List of Columns1']
    measures_2 = measures['List of Columns1']

    #CREATING ISNULL(FILTER COLUMNS)
    dimensions_filter_T['List of Columns1'] = "isnull(" + dimensions_filter_T['List of Columns1'] + ",'')"
    filters_2_T = dimensions_filter_T.loc[dimensions_filter_T['Filter'].notnull(),['List of Columns1','Filter']]
    filters_2_T = filters_2_T.apply(lambda row: ' '.join(row.values.astype(str)), axis=1).str.cat(sep=' AND ')

    #CREATING SELECT GRAINS FROM TABLE 1 for Total Check 
    Grains_Select1_D = "upper(ltrim(rtrim(" + dimensions_1 + "))) "  +  dimensions_1
    Grains_Select1_D = Grains_Select1_D.str.cat(sep=',') 

    #CREATING SELECT GRAINS FROM TABLE 2
    Grains_Select2_D = "upper(ltrim(rtrim(" + dimensions_2 + "))) "  + dimensions_1
    Grains_Select2_D = Grains_Select2_D.str.cat(sep=',') 


    #CREATING GROUP BY GRAINS FROM TABLE 1
    Grains_1_D = dimensions_1.str.cat(sep=',') 

    #CREATING GROUP BY GRAINS FROM TABLE 2
    Grains_2_D = dimensions_2.str.cat(sep=',') 

    # measures_Premiums = measures[measures['type_of_measure']=='Premium']

    #CREATING PREMIUM MEASURES 
    measures_1 = measures['List of Columns']
    measures_2 = measures['List of Columns1']

    #TABLE1
    measures_grain_check_1 = "Sum(" + measures_1 + ") " + measures_2
    measures_grain_check_1 = measures_grain_check_1.str.cat(sep=',') 

    #TABLE2
    measures_grain_check_2 = measures['sign_convention'] + "*-1*Sum(" + measures['List of Columns1'] + ")" + " " + + measures_2
    measures_grain_check_2 = measures_grain_check_2.str.cat(sep=',')
    
     #HAVING CONDITION
    measures_grain_check_H = " Sum(" + measures_1 + ") <> 0 " 
    measures_grain_check_H = measures_grain_check_H.str.cat(sep='OR') 

    measures_grain_check_H_2 = " Sum(" + measures_2 + ") <> 0 " 
    measures_grain_check_H_2 = measures_grain_check_H_2.str.cat(sep='OR') 

    File_Name_Level_1 = dimensions.loc[dimensions['SNo']== file_level,'List of Columns']
    File_Name_Level_2 = dimensions.loc[dimensions['SNo']== file_level,'List of Columns1']

    print("Running Total Check....")

    GrainChecks_qry1 = """SELECT 'Actuarialdb' source, %s ,
            %s 
            FROM  %s c 
            WHERE %s 
            GROUP BY %s
            HAVING (%s)
            """% (Grains_Select1_D, measures_grain_check_1,str5,filters_1_T, Grains_1_D,measures_grain_check_H)

    GrainChecks_qry2 ="""
            SELECT 'SISAD' source, %s ,
                %s 
                FROM  %s
                WHERE %s 
                GROUP BY %s
                HAVING (%s)
            """ % (Grains_Select2_D, measures_grain_check_2,str6,filters_2_T, Grains_2_D,measures_grain_check_H_2)

    GrainChecks_qry_Res1 = pd.read_sql(GrainChecks_qry1, conn)
    GrainChecks_qry_Res2 = pd.read_sql(GrainChecks_qry2, conn1)
    GrainChecks_qry_Res =  pd.concat([GrainChecks_qry_Res1, GrainChecks_qry_Res2])


    column1 = File_Name_Level_1.apply(lambda x: x[1:len(x)-1])
    column2 = File_Name_Level_2.apply(lambda x: x[1:len(x)-1])

    d= pd.Series((GrainChecks_qry_Res[File_Name_Level_1.apply(lambda x: x[1:len(x)-1])].iloc[:,0])).unique()

    from pandas import ExcelWriter
    OBU = ''.join(e for e in obu_names[1:len(obu_names)-1] if e.isalnum())

    print("Running Grain level Checks....")

    type_of_measures = pd.Series(measures['type_of_measure']).unique()
    for i in range(len(d)):
        
        FilePath = r"%s\Granular\%s.xlsx" %(save_file_path ,d[i]) 
        writer = pd.ExcelWriter(FilePath)
        e=GrainChecks_qry_Res[column1] == d[i]
        es = pd.Series(e.iloc[:,0])
        data = GrainChecks_qry_Res[es]
        data.to_excel(writer, sheet_name='Total')
        for j in range(len(type_of_measures)):
            comment = "Running %s Grain level Checks for %s...." %(type_of_measures[j],d[i])
            print(comment)
            measures_G = measures[measures['type_of_measure']==type_of_measures[j]]
            measures_2 = measures_G['List of Columns1']
            dimensions_1.reset_index(drop=True,inplace = True)
          
            for k in range(len(dimensions_1)):
                individual_grain = pd.Series('source').append(pd.Series(dimensions_1[k][1:len(dimensions_1[k])-1]))
                Individual_Check_columns = individual_grain.append(measures_2.apply(lambda x: x[1:len(x)-1]))           
                GrainChecks_qry_Res_P = data[Individual_Check_columns]    
                sheet_name = type_of_measures[j][0] + '-' + ''.join(e for e in dimensions_1[k][1:len(dimensions_1[k])-1] if e.isalnum())
                GrainChecks_qry_Res_P= GrainChecks_qry_Res_P.fillna('dummy')
                table = pd.pivot_table(GrainChecks_qry_Res_P,index= dimensions_1[k][1:len(dimensions_1[k])-1],values=measures_2.apply(lambda x: x[1:len(x)-1]),columns= ["source"],aggfunc=[np.sum], margins=True,fill_value=0).reset_index().replace('dummy',np.nan).set_index(dimensions_1[k][1:len(dimensions_1[k])-1])
                table.to_excel(writer, sheet_name=sheet_name)
        writer.save()
    
    End_Time = datetime.datetime.now()
    print("Granular Check is complete in: ") 
    print(End_Time-Start_Time)

def runAll():
    granularCheck()
    debugCheck()

def rw_ui():
    global df,recon_type
    global call,app    
    nb_row = df.shape[0]
    nb_col = df.shape[1]
    str1=''
    for row in range (nb_row):
            for col in range(nb_col):
                df.iloc[row,col]=call.Table1.item(row, col).text()
                # print(call.Table1.item(row, col).text())
    
    df.fillna('',inplace=True)
    conn = odbc.connect('Driver={SQL Server};'
                      'Server=actuarialdb;'
                      'Database=actuarialdb;'
                      'Trusted_Connection=yes;')

    del_q="DELETE from dbo.rt_t1100_DataReconMappings where Type_Of_recon ='"+recon_type+"'"
    conn.execute(del_q)
    for row in range (nb_row):
        str1=''
        for col in range(nb_col):
            # columns1=columns1.str.replace("[","'")
            str1=str1+df.iloc[row,col].replace("'","$")+"','"
            
        string=str1[:-2] 
        string=string.replace("'-999'","NULL")
        string=string.replace("''","NULL")
        cursor=conn.cursor()
        qry="INSERT INTO dbo.rt_t1100_DataReconMappings([SNo], [List of Columns],[List of Columns1],[Measure/Dimension],[Filter],[NULL Check],[Distinct Check],[Granularity],[Priority],[Length Check],[Datatype Check],[sign_convention],[type_of_measure],[type_of_recon]) "
        qry=qry+" values('"+string+",'"+recon_type+"')" 
        # print(qry)   
        cursor.execute(qry)
        cursor.execute("update dbo.rt_t1100_DataReconMappings set filter =replace(filter,'$','''') where Type_Of_recon = '" +recon_type + "'")
        cursor.commit()
        cursor.close()
    app.exec_()
    # app=QtWidgets.QApplication([])
    call=uic.loadUi("rw.ui")
    call.show()
    app.exec()
    call.pushButton2.clicked.connect(clearcombo)
    call.saveserver.clicked.connect(getdb)
    call.savedb.clicked.connect(gettables)
    call.savetable.clicked.connect(getall)
    call.pushButton.clicked.connect(runAll)
    call.addfilter.clicked.connect(filteraddition)
    call.addmeasure.clicked.connect(filteradditionmeasure)
    call.confirmfilter.clicked.connect(filterfinal)
    call.confirmmeasure.clicked.connect(filterfinalmeasure)
    call.confirmfilter0.clicked.connect(setfiltercolumn)
    call.editmappings.clicked.connect(table_ui)
    call.woediting.clicked.connect(withoutediting)

def addrow():
    data=''
    global df
    call.Table1.setRowCount(df.shape[0]+1)
    # dff=df.copy()
    df.loc[df.iloc[-1].name + 1,:] = np.nan
    df.fillna('',inplace=True)
    for row in range (df.shape[0]-1, df.shape[0]):
        for col in range (0, df.shape[1]):
            data = str(df.iat[row, col] )
            call.Table1.setItem(row, col, item(str(data)))

    # df.loc[df.iloc[-1].name + 1,:] = np.nan


call.pushButton2.clicked.connect(clearcombo)
call.saveserver.clicked.connect(getdb)
call.savedb.clicked.connect(gettables)
call.savetable.clicked.connect(getall)
call.pushButton.clicked.connect(runAll)
call.addfilter.clicked.connect(filteraddition)
call.addmeasure.clicked.connect(filteradditionmeasure)
call.confirmfilter.clicked.connect(filterfinal)
call.confirmmeasure.clicked.connect(filterfinalmeasure)
call.confirmfilter0.clicked.connect(setfiltercolumn)
call.editmappings.clicked.connect(table_ui)
call.woediting.clicked.connect(withoutediting)

call.show()
app.exec()
