# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 14:31:05 2014

@author: bcaine
"""

import os
import csv
from datetime import datetime
#from time import strftime
#from os import listdir
from os.path import isfile, join, getmtime
import sys
import re
import pyodbc
import time
#from tornado import gen
import itertools

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
def process_step_data(f, cursor, row, amp_hr, watt_hr, file_id):
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

    s = """
    insert into CycleStep (file_id, rec_num, cycle_num, step, test_sec, step_sec, amp_hr, watt_hr, amps, volts, state, ES, dpt_time, WF_chg_cap, WF_dis_cap, WF_chg_E, WF_dis_E, aux1, units)
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

#    s = create_merge_str('CycleStep', 'S.file_id = T.file_id and S.rec_num <= T.rec_num',
#                         'file_id', 'rec_num', 'cycle_num', 'step', 'test_sec', 'step_sec', 'amp_hr', 'watt_hr', 'amps', 'volts', 'state', 'ES', 'dpt_time',
#                         'WF_chg_cap', 'WF_dis_cap', 'WF_chg_E', 'WF_dis_E', 'aux1', 'units')
    cursor.execute(s, file_id, row['Rec#'], row['Cyc#'], row['Step'], row['Test (Sec)'], row['Step (Sec)'], amp_hr, watt_hr, row['Amps'], row['Volts'], row['State'], row['ES'], row['DPt Time'],
                   wfchgcap, wfdiscap, wfchge, wfdise, aux1, units)
                  
####################################

# Add records to db, given the file myFile, filename f, and cursor cursor.
#@gen.engine
def add_to_db(myFile, f, cursor, dirpath):
    ################ FILE DATA ################
    fdstart = datetime.now()
    #dialect = csv.Sniffer().sniff(myFile.read())

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
        else:
            return
    elif old_trq_match:
        test_req = old_trq_match.group('trq')
        # look for A-Z cell idx in file name
        cell_index = old_trq_match.group('cell_index')
        # convert A-Z cell idx to 1-26 number
        cell_index = ord(cell_index.lower()) - 96
    else:
        return
        
    # look for cycle type in the file name
    if 'form' in f.lower():
        cycle_type = 'form'
    else:
        cycle_type = 'cyc'
    # If it's "Fukushima" and nothing else, just label it as Fukushima.
#    if cycle_type == 'other' and 'fukushima' in f.lower():
#        cycle_type = 'fukushima'

    # Get first row (fieldnames)
    #reader = csv.DictReader(myFile, dialect=dialect, delimiter='\t')
    reader = csv.DictReader(myFile, delimiter='\t')
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
    lotname = None
    lot_tmp = fieldnames[4]
    index = lot_tmp.find('Barcode: ')
    lot_code = lot_tmp[index+9:]
    m = re.search('(?P<lotname>[A-Za-z]{5}[0-9]{2}[A-Za-z])[0-9]{4}', lot_code)
    if m:
        lotname = m.group('lotname')
        
    before_merges = datetime.now()

    # Add row to CellLot table.
    s = create_merge_str('CellLot', False,
                         'TestRequest', 'CellLotName')
    cursor.execute(s, test_req, lotname)

    # Add row to CellAssembly table.
    cell_lot_uid = None
    q = cursor.execute('select CellLotUID from CellLot where TestRequest = ?', test_req).fetchone()
    if q:
        cell_lot_uid = q[0]

    s = create_merge_str('CellAssembly', 'S.CellLotUID = T.CellLotUID and S.CellIndex = T.CellIndex',
                         'CellLotUID', 'CellIndex')
    cursor.execute(s, cell_lot_uid, cell_index)

    after_merges = datetime.now() - before_merges
    print 'merges: ' + str(after_merges)
    before_misc = datetime.now()

    # Add row to CycleFile
    cell_assy_uid = None
    q_row = cursor.execute("""
    select CellAssyUID from CellAssembly
    inner join CellLot
    on CellAssembly.CellLotUID = CellLot.CellLotUID
    where CellLot.TestRequest = ? and CellAssembly.CellIndex = ?
    """, test_req, cell_index).fetchone()
    if q_row:
        cell_assy_uid = q_row[0]
    s = """insert into CycleFile (CellAssyUID, Filename, TestDate, ProcedureName)
    values (?,?,?,?)
    """
#    s = create_merge_str('CycleFile', 'S.Filename = T.Filename',
#                         'CellAssyUID', 'Filename', 'TestDate', 'ProcedureName')
    cursor.execute(s, cell_assy_uid, f, test_date, proc_nm)

    # get file id for this file
    file_id = -1
    q_row = cursor.execute("""
    select FileId from CycleFile
    where Filename = ?
    """, f).fetchone()
    if q_row:
        file_id = q_row[0]

#    reader = csv.DictReader(myFile, dialect=dialect, delimiter= '\t')
    reader = csv.DictReader(myFile, delimiter='\t')
    
    # Get last rec_num in db for this file
    last_prev_rec_num = -1
    q = cursor.execute("""
    select top 1 rec_num from CycleStep
    where file_id = ?
    order by rec_num desc
    """, file_id).fetchone()
    if q:
        last_prev_rec_num = q[0]

    after_misc = datetime.now() - before_misc
    print 'misc: ' + str(after_misc)
    
    print 'all file data: ' + str(datetime.now() - fdstart)
            
    ################## STEP DATA ##################

    bf_step = datetime.now()
    # Record all beginning and end of step rows
    step_zero_amp_hr = 0
    step_zero_watt_hr = 0
    for row in reader:
        # go to the last record we've been to in this file.
        if int(row['Rec#']) <= last_prev_rec_num:
            break
        # Beginning or end of step
        if int(row['ES']) >= 128 or int(row['ES']) == 0:
            # Zero out the amp hr and watt hr counters, store values in variables.
            if int(row['ES']) == 0:
                step_zero_amp_hr = float(row['Amp-hr'])
                step_zero_watt_hr = float(row['Watt-hr'])
                # If step zero values are less than 10^-5, just set them to 0.
                if step_zero_amp_hr < 0.00001:
                    step_zero_amp_hr = 0
                if step_zero_watt_hr < 0.00001:
                    step_zero_watt_hr = 0
                amp_hr, watt_hr = 0,0
            # Add stored values to amp hr and watt hr.
            elif int(row['ES']) >= 128:
                amp_hr = abs(float(row['Amp-hr']) - step_zero_amp_hr)
                watt_hr = abs(float(row['Watt-hr']) - step_zero_watt_hr)
            ##################################################
            process_step_data(f, cursor, row, amp_hr, watt_hr, file_id)
            ##################################################

    print 'step data: ' + str(datetime.now() - bf_step)
    # Call stored procedure to populate CellCycle table and cell_cycle_uid columns of CycleStep
    ####################################
    #before = datetime.now()
    cursor.execute("exec FillCellCycle @filename = '" + f + "', @cell_assy_uid = " + str(cell_assy_uid) + ", @cycle_type_name = '" + cycle_type + "'")
#    while 1:
#        q = cursor.execute('select status from RunningStatus').fetchone()
#        print 'q[0] = ' + str(q[0])
#        if q[0] == 0:
#            break
    time.sleep(float(os.path.getsize(os.path.join(dirpath, f)))/10000000) # make sure the process can finish
    #print 'exec time: ', datetime.now()-before
    ####################################

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

basePath = r'\\24m-fp01\24m\\MasterData\Battery_Tester_Backup\24MBattTester_Maccor\Data\ASCIIfiles\MACCOR-M'
#basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\data\\ASCIIfiles\\TestFiles'

errorFiles = []

sys.stdout.write('Working')

# search folders and subfolders and call add_to_db.
for dirpath, dirnames, filenames in os.walk(basePath):
    for f in filenames:
        beginfile = datetime.now()
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
                add_to_db(myFile, f, cursor, dirpath)
                #############################
        except csv.Error, e:
            errorFiles.append(f)
            continue
        
        print 'total file time: ' + str(datetime.now() - beginfile)

print "\nThese files didn't process: ", errorFiles

#close up shop
cursor.close()
del cursor
cnxn.close()
#
## For "last ran" functionality on website
#with open(r'C:\Users\bcaine\Documents\My Web Sites\EmptySite\globals\form_last_updated.php', 'w') as f:
#    f.write('Last ran on ' + strftime('%Y-%m-%d %H:%M:%S'))

print (datetime.now() - startTime)