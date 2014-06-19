# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 14:31:05 2014

@author: bcaine
"""

import os
import csv
from datetime import datetime
from time import strftime
from os import listdir
from os.path import isfile, join, getmtime
from collections import OrderedDict
import sys
import re
import pyodbc

startTime = datetime.now()

#class CycleStep:
#    def __init__(self, filename, test_req, cell_index, test_date, cycle_type, 
#                 rec_num, cycle_num, step, test_sec, step_sec, amp_hr, watt_hr, amps, volts, state, ES, dpt_time,
#                 WF_chg_cap, WF_dis_cap, WF_chg_E, WF_dis_E, aux1, units):
#        self.filename = filename
#        self.test_req = test_req
#        self.cell_index = cell_index
#        self.test_date = test_date
#        self.cycle_type = cycle_type
#        self.rec_num = rec_num
#        self.cycle_num = cycle_num
#        self.step = step
#        self.test_sec = test_sec
#        self.step_sec = step_sec
#        self.amp_hr = amp_hr
#        self.watt_hr = watt_hr
#        self.amps = amps
#        self.volts = volts
#        self.state = state
#        self.ES = ES
#        self.dpt_time = dpt_time
#        self.WF_chg_cap = WF_chg_cap
#        self.WF_dis_cap = WF_dis_cap
#        self.WF_chg_E = WF_chg_E
#        self.WF_dis_E = WF_dis_E
#        self.aux1 = aux1
#        self.units = units
#    def __str__(self):
#        s = 'filename: ' + str(self.filename) + '\n'
#        s += 'test_req: ' + str(self.test_req) + '\n'
#        s += 'cell_index: ' + str(self.cell_index) + '\n'
#        s += 'test_date: ' + str(self.test_date) + '\n'
#        s += 'cycle_type: ' + str(self.cycle_type) + '\n'
#        s += 'rec_num: ' + str(self.rec_num) + '\n'
#        s += 'cycle_num: ' + str(self.cycle_num) + '\n'
#        s += 'step: ' + str(self.step) + '\n'
#        s += 'test_sec: ' + str(self.test_sec) + '\n'
#        s += 'step_sec: ' + str(self.step_sec) + '\n'
#        s += 'amp_hr: ' + str(self.amp_hr) + '\n'
#        s += 'watt_hr: ' + str(self.watt_hr) + '\n'
#        s += 'amps: ' + str(self.amps) + '\n'
#        s += 'volts: ' + str(self.volts) + '\n'
#        s += 'state: ' + str(self.state) + '\n'
#        s += 'ES: ' + str(self.ES) + '\n'
#        s += 'dpt_time: ' + str(self.dpt_time) + '\n'
#        s += 'WF_chg_cap: ' + str(self.WF_chg_cap) + '\n'
#        s += 'WF_dis_cap: ' + str(self.WF_dis_cap) + '\n'
#        s += 'WF_chg_E: ' + str(self.WF_chg_E) + '\n'
#        s += 'WF_dis_E: ' + str(self.WF_dis_E) + '\n'
#        s += 'aux1: ' + str(self.aux1) + '\n'
#        s += 'units: ' + str(self.units) + '\n'
#        return s

######### SCRAPE ASCII FILES FOR DATA ##########

# connect to db
cnxn_str = """
Driver={SQL Server Native Client 11.0};
Server=172.16.111.235\SQLEXPRESS;
Database=CellTestData;
UID=sa;
PWD=Welcome!;
"""
cnxn = pyodbc.connect(cnxn_str)
cnxn.autocommit = True
cursor = cnxn.cursor()

#basePath = r'\\24m-fp01\24m\\MasterData\Battery_Tester_Backup\24MBattTester_Maccor\Data\ASCIIfiles\MACCOR-M'
basePath = 'C:\\Users\\bcaine\\Desktop\\Dummy Maccor Data\\data\\ASCIIfiles\\TestFiles\\supertest'

errorFiles = []

#cycleSteps = []

sys.stdout.write('Working')

# search folders and subfolders
for dirpath, dirnames, filenames in os.walk(basePath):
    for f in filenames:
        # check last update, skip if already in FileUpdate db
        date = datetime.fromtimestamp(getmtime(os.path.join(dirpath, f))).strftime("%Y-%m-%d %H:%M:%S")
        row = cursor.execute("""
        select * from RawStepFileUpdate
        where Filename = ? and LastUpdate = ?;
        """, f, date).fetchone()
        if row:
            sys.stdout.write('^')
            continue

        # Look for test req in file name. Do not skip.
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

        # look for cycle type in the file name
        cycle_type = 'other'
        cycletypes = ['form', 'cyc', 'test', 'eol', 'rpt', 'disch', 'rate']
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

        try:
            with open(os.path.join(dirpath, f), 'rb') as myFile:
                dialect = csv.Sniffer().sniff(myFile.read())
                reader = csv.DictReader(myFile, dialect=dialect,delimiter='\t')
                myFile.seek(0)
                fieldnames = reader.fieldnames
                # test date: B1
                test_date = fieldnames[1]
#                print (datetime.now() - startTime)
#                print 'before RawCycleFile merge'
                cursor.execute("""
                merge RawCycleFile as T
                using (select ?,?,?,?,?)
                as S (filename, test_req, cell_index, test_date, cycle_type)
                on S.filename = T.filename
                when not matched then insert (filename, test_req, cell_index, test_date, cycle_type)
                values (S.filename, S.test_req, S.cell_index, S.test_date, S.cycle_type);
                """, f, test_req, cell_index, test_date, cycle_type)
#                print (datetime.now() - startTime)
#                print 'after RawCycleFile merge'
#                # lot code: E1
#                lot_tmp = fieldnames[4]
#                index = lot_tmp.find('Barcode: ')
#                lot_code = lot_tmp[index+9:]
                
                reader = csv.DictReader(myFile, dialect=dialect,delimiter= '\t')
                
                # Record all beginning and end of step rows                
                for row in reader:
                    # Beginning or end of step
                    if int(row["ES"]) >= 128 or int(row["ES"]) == 0:
                        # Put the row into the array cycleSteps
                        # aux1 and units not in every file
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
#                        print (datetime.now() - startTime)
#                        print 'before getting file_id'
                        file_id = -1
                        q_row = cursor.execute("""
                        select file_id from RawCycleFile
                        where filename = ?
                        """, f).fetchone()
                        if q_row:
                            file_id = q_row[0]
#                        print (datetime.now() - startTime)
#                        print 'after getting file_id'
                            
                        cursor.execute("""
                        merge RawCycleStep as T
                        using (select ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        as S (file_id, rec_num, cycle_num, step, test_sec, step_sec, amp_hr, watt_hr, amps, volts, state, ES, dpt_time,
                                     WF_chg_cap, WF_dis_cap, WF_chg_E, WF_dis_E, aux1, units)
                        on S.file_id = T.file_id and S.rec_num <= T.rec_num
                        when not matched then insert (file_id, rec_num, cycle_num, step, test_sec, step_sec, amp_hr, watt_hr, amps, volts, state, ES, dpt_time,
                                     WF_chg_cap, WF_dis_cap, WF_chg_E, WF_dis_E, aux1, units)
                        values (S.file_id, S.rec_num, S.cycle_num, S.step, S.test_sec, S.step_sec, S.amp_hr, S.watt_hr, S.amps, S.volts, S.state, S.ES, S.dpt_time,
                                     S.WF_chg_cap, S.WF_dis_cap, S.WF_chg_E, S.WF_dis_E, S.aux1, S.units);
                        """, file_id, row['Rec#'], row['Cyc#'], row['Step'], row['Test (Sec)'], row['Step (Sec)'], row['Amp-hr'], row['Watt-hr'], row['Amps'], row['Volts'], row['State'], row['ES'], row['DPt Time'],
                                  wfchgcap, wfdiscap, wfchge, wfdise, aux1, units)
#                        print (datetime.now() - startTime)
#                        print 'after pushing to db'

                # All ok, so add row to FileUpdate table
                cursor.execute("""
                merge RawStepFileUpdate as T
                using (select ?, ?) as S (Filename, LastUpdate)
                on S.Filename = T.Filename and S.LastUpdate = T.LastUpdate
                when not matched then insert(Filename, LastUpdate)
                values (S.Filename, S.LastUpdate);
                """, f, date)
                
                sys.stdout.write('.')
        
        except csv.Error, e:
            errorFiles.append(f)
            continue

#for c in cycleSteps:
#    print c

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