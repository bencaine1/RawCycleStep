# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 14:31:05 2014

@author: bcaine
"""

import os
import csv
from datetime import datetime
from datetime import timedelta
from time import strftime
from os import listdir
from os.path import isfile, join, getmtime
from collections import OrderedDict
import sys
import re
import pyodbc

startTime = datetime.now()

def parsetime(dts):
    try:
        d = datetime.strptime(dts, '%m/%d/%Y %H:%M')
        return d
    except:
        d = datetime.strptime(dts, '%m/%d/%Y %H:%M:%S')
        return d
        
def create_merge_str(table_name, special_merge, *args):
    s = 'merge ' + table_name + ' as T '
    s += 'using (select '
    for i in range(len(args)):
        s += '?,'
    s = s.rstrip(',')
    s += ') as S ('
    for c in args:
        s += c
        s += ','
    s = s.rstrip(',')
    s += ') on '
    if (special_merge):
        s += special_merge
    else:
        s += 'S.'
        s += args[0]
        s += ' = T.'
        s += args[0]
    s += ' when not matched then insert ('
    for c in args:
        s += c
        s += ','
    s = s.rstrip(',')
    s += ') values ('
    for c in args:
        s += 'S.'
        s += c
        s += ','
    s = s.rstrip(',')
    s += ');'
    return s

####################################

# Add row to CycleStep table.
# Easy. Given cycle_id, just merge all the values in the current row into the table.
def process_step_data(f, cursor, row, amp_hr, watt_hr):
    # Put the row into the array cycleSteps
    try:
        wfchgcap = row['WF Chg Cap']
        if wfchgcap == 'N/A':
            wfchgcap = None
    except:
        wfchgcap = None
    try:
        wfdiscap = row['WF Dis Cap']
        if wfdiscap == 'N/A':
            wfdiscap = None
    except:
        wfdiscap = None
    try:
        wfchge = row['WF Chg E']
        if wfchge == 'N/A':
            wfchge = None
    except:
        wfchge = None
    try:
        wfdise = row['WF Dis E']
        if wfdise == 'N/A':
            wfdise = None
    except:
        wfdise = None
    try:
        aux1 = row['Aux #1']
    except:
        aux1 = None
    try:
        units = row[' Units']
    except:
        units = None

    file_id = -1
    q_row = cursor.execute("""
    select FileId from CycleFile
    where Filename = ?
    """, f).fetchone()
    if q_row:
        file_id = q_row[0]

    s = create_merge_str('CycleStep', 'S.file_id = T.file_id and S.rec_num <= T.rec_num',
                         'file_id', 'rec_num', 'cycle_num', 'step', 'test_sec', 'step_sec', 'amp_hr', 'watt_hr', 'amps', 'volts', 'state', 'ES', 'dpt_time',
                         'WF_chg_cap', 'WF_dis_cap', 'WF_chg_E', 'WF_dis_E', 'aux1', 'units')
    cursor.execute(s, file_id, row['Rec#'], row['Cyc#'], row['Step'], row['Test (Sec)'], row['Step (Sec)'], amp_hr, watt_hr, row['Amps'], row['Volts'], row['State'], row['ES'], row['DPt Time'],
                   wfchgcap, wfdiscap, wfchge, wfdise, aux1, units)
              
####################################

#def process_cycle_data(cursor):
#    # for now fill with dummies
#    s = create_merge_str('CellCycle', False, 'CellAssyUID', 'CycleNum')
#    cursor.execute(s, 1, 1)

####################################
              
# Add row to CycleFile table, and CellLot and CellAssy if applicable.
# This function is long, but easy.
def process_file_data(myFile, f, cursor, dialect):
    # Look for test req in file name. Skip if not present.
    test_req, cell_index = None, None
    test_req_match = re.search('_(?P<number>[0-9]{6})_', f)
    old_trq_match = re.search('(?P<trq>[0-9]{4}_[0-9]{3})(?P<cell_index>[a-zA-Z])', f)
    if test_req_match:
        test_req = test_req_match.group('number')
        # look for 4-digit cell idx in file name
        cell_index_match = re.search('_(?P<number>[0-9]{4})[^0-9]', f)
        if cell_index_match:
            cell_index = cell_index_match.group('number')                
    elif old_trq_match:
        test_req = old_trq_match.group('trq')
        # look for A-Z cell idx in file name
        cell_index = old_trq_match.group('cell_index')
    else:
        return

    # look for cycle type in the file name
    cycle_type = 'other'
    cycletypes = ['form', 'cyc', 'eol', 'rpt', 'disch', 'rate']
    for t in cycletypes:
        if t in f.lower():
            cycle_type = t
    if cycle_type == 'form':
        if ('form01' or 'form1') in f:
            cycle_type = 'form1'
        elif ('form02' or 'form2') in f:
            cycle_type = 'form2'
    # If it's "Fukushima" and nothing else, just label it as Fukushima.
    elif cycle_type == 'other' and 'fukushima' in f.lower():
        cycle_type = 'fukushima'

    # Get first row (fieldnames)
    reader = csv.DictReader(myFile, dialect=dialect, delimiter='\t')
    myFile.seek(0)
    fieldnames = reader.fieldnames

    # Procedure name: D1
    procTemp = fieldnames[3]
    index = procTemp.find('Procedure:')
    index2 = procTemp.find('.000')
    proc_nm = procTemp[index+11:index2]

    # test date: B1
    test_date = fieldnames[1]
    
    # lot code: E1
    sn = None
    lot_tmp = fieldnames[4]
    index = lot_tmp.find('Barcode: ')
    lot_code = lot_tmp[index+9:]
    m = re.search('(?P<sn>[A-Za-z]{5}[0-9]{2}[A-Za-z])[0-9]{4}', lot_code)
    if m:
        sn = m.group('sn')

    s = create_merge_str('CellAssembly', 'S.CellLotName = T.CellLotName and S.TestRequest = T.TestRequest',
                         'CellLotName', 'TestRequest', 'CellAssySN')
    cursor.execute(s, cell_index, test_req, sn)
    
    # Add row to AssemblyProcess (if new)
    q_row = cursor.execute("""select CellAssyUID from CellAssembly
    where CellAssembly.CellLotName = ? and CellAssembly.TestRequest = ?
    """, cell_index, test_req).fetchone()
    if q_row:
        cell_assy_uid = q_row[0]
    s = create_merge_str('AssemblyProcess', 'S.CellAssyUID = T.CellAssyUID and S.CycleType = T.CycleType',
                         'CellAssyUID', 'CycleType')
    cursor.execute(s, cell_assy_uid, cycle_type)

    # Add row to CycleFile
    q_row = cursor.execute("""select AssemblyProcessUID from AssemblyProcess
    inner join CellAssembly
    on CellAssembly.CellAssyUID = AssemblyProcess.CellAssyUID
    where CellAssembly.CellLotName = ? and CellAssembly.TestRequest = ? and AssemblyProcess.CycleType = ?
    """, cell_index, test_req, cycle_type).fetchone()
    if q_row:
        assy_proc_uid = q_row[0]
    s = create_merge_str('CycleFile', 'S.Filename = T.Filename',
                         'AssemblyProcessUID', 'Filename', 'TestDate', 'ProcedureName')
    cursor.execute(s, assy_proc_uid, f, test_date, proc_nm)
                  
####################################

# Add records to db, given the file myFile, filename f, and cursor cursor.
def add_to_db(myFile, f, cursor):
    dialect = csv.Sniffer().sniff(myFile.read())

    process_file_data(myFile, f, cursor, dialect)

    reader = csv.DictReader(myFile, dialect=dialect, delimiter= '\t')
    
    # Record all beginning and end of step rows
#    hasbeen133C = False
#    raw_cycle_num, cyc_begin, cyc_end = -1, None, None
#    is_firstrow = True
    step_zero_amp_hr = 0
    step_zero_watt_hr = 0
    for row in reader:
#        if is_firstrow:
#            cyc_begin = parsetime(row['DPt Time'])
        # Beginning or end of step
        if int(row['ES']) >= 128 or int(row['ES']) == 0:
            # Zero out the amp hr and watt hr counters, store values in variables.
            if int(row['ES']) == 0:
                step_zero_amp_hr = float(row['Amp-hr'])
                step_zero_watt_hr = float(row['Watt-hr'])
                amp_hr, watt_hr = 0,0
            # Add stored values to amp hr and watt hr.
            elif int(row['ES']) >= 128:
                amp_hr = float(row['Amp-hr']) - step_zero_amp_hr
                watt_hr = float(row['Watt-hr']) - step_zero_watt_hr
                
            #process_cycle_data(cursor)
#            if row['Cyc#'] > raw_cycle_num:
#                raw_cycle_num = row['Cyc#']
#                cyc_time = parsetime(row['DPt Time']) - cyc_begin
#                if cyc_time > timedelta(hours=1):
#                    real_cycle_num+=1
#                    
#            if int(row['ES']) == 133 and row['State'] == 'C':
#                hasbeen133C = True
#            elif int(row['ES']) == 132 and row['State'] == 'C' and hasbeen133C == True:
#                
#                hasbeen133C = False
                
#                        if last133D:
#                            q_row = cursor.execute("""
#                            SELECT TOP 1 raw_step_id FROM RawCycleStep
#                            ORDER BY raw_step_id DESC
#                            """)
#                            if q_row:
#                                raw_step_id = q_row[0]
#                            cursor.execute("""
#                            merge RawCellCycle as T
#                            using (select ?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#                            as S (raw_step_id, capacity_charge, capacity_discharge,
#                                  mAhCharge, mAhDischarge, mWhCharge, mWhDischarge,
#                                  ACharge, CapRet, EnEff, Vch, Vdis, ASI, InitialCapacity)
#                            on S.raw_step_id = T.raw_step_id
#                            when not matched then insert (raw_step_id, capacity_charge, capacity_discharge,
#                                                          mAhCharge, mAhDischarge, mWhCharge, mWhDischarge,
#                                                          ACharge, CapRet, EnEff, Vch, Vdis, ASI, InitialCapacity)
#                            values (S.raw_step_id, S.capacity_charge, S.capacity_discharge,
#                                    S.mAhCharge, S.mAhDischarge, S.mWhCharge, S.mWhDischarge,
#                                    S.ACharge, S.CapRet, S.EnEff, S.Vch, S.Vdis, S.ASI, S.InitialCapacity)
#                            """, raw_step_id, None, None, None, None, None, None, None, None, None, None, None, None, None)
#                            last133D = None
#                    elif row(['State']) == 'D':
#                        last133D = row['Amp-hr']
#                        if last133C:
#                            row = cursor.execute("""
#                            SELECT TOP 1 raw_step_id FROM RawCycleStep
#                            ORDER BY raw_step_id DESC
#                            """)
                
            process_step_data(f, cursor, row, amp_hr, watt_hr)
#        is_firstrow = False
    # All ok, so add row to FileUpdate table
    cursor.execute("""
    merge FileUpdate as T
    using (select ?, ?) as S (Filename, LastUpdate)
    on S.Filename = T.Filename and S.LastUpdate = T.LastUpdate
    when not matched then insert(Filename, LastUpdate)
    values (S.Filename, S.LastUpdate);
    """, f, date)
    
    sys.stdout.write('.')

################################
############ MAIN ##############
################################

# connect to db
cnxn_str = """
Driver={SQL Server Native Client 11.0};
Server=172.16.111.235\SQLEXPRESS;
Database=CellTestData2;
UID=sa;
PWD=Welcome!;
"""
cnxn = pyodbc.connect(cnxn_str)
cnxn.autocommit = True
cursor = cnxn.cursor()

#basePath = r'\\24m-fp01\24m\\MasterData\Battery_Tester_Backup\24MBattTester_Maccor\Data\ASCIIfiles\MACCOR-M'
basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\data\\ASCIIfiles\\TestFiles'

errorFiles = []

#cycleSteps = []

sys.stdout.write('Working')

# search folders and subfolders and call add_to_db.
for dirpath, dirnames, filenames in os.walk(basePath):
#    real_cycle_num = 0
    for f in filenames:
        # check last update, skip if already in FileUpdate db
        date = datetime.fromtimestamp(getmtime(os.path.join(dirpath, f))).strftime("%Y-%m-%d %H:%M:%S")
        row = cursor.execute("""
        select * from FileUpdate
        where Filename = ? and LastUpdate = ?;
        """, f, date).fetchone()
        if row:
            sys.stdout.write('^')
            continue

        # Open the file and add to the db if necessary.
        try:
            with open(os.path.join(dirpath, f), 'rb') as myFile:
                #############################
                add_to_db(myFile, f, cursor)
                #############################
        except csv.Error, e:
            errorFiles.append(f)
            continue

"""
for each AssemblyProcessUID:
	find all files with that AssemblyProcessUID
	order those files by filename
	for each of those files:
		when cycle_num is incremented, if the cycle lasted longer than an hour, make a new CellCycle record with the next cycle_num
"""

# assy_proc: all assembly procedures.
# e.g. for a given trq/cellidx, it could contain the files cyc01, cyc02, and cyc03.
assy_proc = cursor.execute("""
select * from AssemblyProcess
""").fetchall()
for row in assy_proc:
    # files: all files in a given assy_proc. e.g. cyc01, cyc02, cyc03.
    files = cursor.execute("""
    select FileID, Filename from CycleFile
    where AssemblyProcessUID = ?
    order by Filename
    """, row.AssemblyProcessUID).fetchall()
    # real_cycle_num: the actual cycle number of the whole process.
    # so if real_cycle_num = 21 at the end of cyc01, it would start from 22 at the beginning of cyc02.
    real_cycle_num = 1
    for f in files:
        # max_cycle_num: the number of cycles in a file.
        max_cycle_num = 0
        q = cursor.execute("""
        select cycle_num from CycleStep
        where file_id = ?
        order by cycle_num desc
        """, f.FileID).fetchone()
        if q:
            max_cycle_num = q[0]
        # For each raw cycle number in a file:
        for i in xrange(max_cycle_num):
            # check the beginning and end dpt_time to see if it's over an hour difference.
            q = cursor.execute("""
            select dpt_time from CycleStep
            where file_id = ? and cycle_num = ?
            order by dpt_time asc
            """, f.FileID, i).fetchone()
            if q:
                start_time = q.dpt_time
            q = cursor.execute("""
            select dpt_time from CycleStep
            where file_id = ? and cycle_num = ?
            order by dpt_time desc
            """, f.FileID, i).fetchone()
            if q:
                end_time = q.dpt_time
            if end_time - start_time > timedelta(hours=1):
                # Add record to CycleNum.
                s = create_merge_str('CellCycle', 'S.AssemblyProcessUID = T.AssemblyProcessUID and S.CycleNum = T.CycleNum',
                                        'AssemblyProcessUID','CycleNum')
                cursor.execute(s, row.AssemblyProcessUID, real_cycle_num)
                # Add cell_cycle_id field to record in Cycle Step.
                q = cursor.execute("""
                select CellCycleUID from CellCycle
                where AssemblyProcessUID = ? and CycleNum = ?
                """, row.AssemblyProcessUID, real_cycle_num).fetchone()
                if q:
                    cell_cycle_id = q.CellCycleUID
                cursor.execute("""
                merge CycleStep as T
                using (select ?,?) as S (file_id, cycle_num)
                on S.file_id = T.file_id and S.cycle_num = T.cycle_num
                when matched then update set cell_cycle_id = ?;
                """, f.FileID, i, cell_cycle_id)
                real_cycle_num += 1


print "\nThese files didn't process: ", errorFiles

########## ADD TO DB ###########

# Delete tables if 'delete' passed in as arg.
#if len(sys.argv) > 1 and sys.argv[1] == 'delete':
#    cursor.execute("""
#    delete from CellCycle;
#    delete from CellAssembly;
#    delete from TestRequest;    
#    """)

# Populate RawCycleStep table
#print 'Populating RawCycleStep table...'
#filenamelist = []
#for c in cycleSteps:
#    if c.filename not in filenamelist:
#        # Delete all
#        cursor.execute("""
#        delete from RawCycleStep
#        where filename = ?
#        """, c.filename)
#        filenamelist.append(c.filename)
#            
#    cursor.execute("""
#    insert into RawCycleStep (filename, test_req, cell_index, test_date, cycle_type, 
#                 rec_num, cycle_num, step, test_sec, step_sec, amp_hr, watt_hr, amps, volts, state, ES, dpt_time,
#                 WF_chg_cap, WF_dis_cap, WF_chg_E, WF_dis_E, aux1, units)
#    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
#    """, c.filename, c.test_req, c.cell_index, c.test_date, c.cycle_type, 
#                 c.rec_num, c.cycle_num, c.step, c.test_sec, c.step_sec, c.amp_hr, c.watt_hr, c.amps, c.volts, c.state, c.ES, c.dpt_time,
#                 c.WF_chg_cap, c.WF_dis_cap, c.WF_chg_E, c.WF_dis_E, c.aux1, c.units)

#close up shop
cursor.close()
del cursor
cnxn.close()
#
## For "last ran" functionality on website
#with open(r'C:\Users\bcaine\Documents\My Web Sites\EmptySite\globals\form_last_updated.php', 'w') as f:
#    f.write('Last ran on ' + strftime('%Y-%m-%d %H:%M:%S'))

print (datetime.now() - startTime)