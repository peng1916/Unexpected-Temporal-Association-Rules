#!/usr/bin/python

'''
module name: ObservationalData
'''

import sys
import MySQLdb
import datetime


class MedicalRecord:
    
    def __init__(self, id, func_date, prescription_records = [], diagnosis_records = []):
        
        self.id = id
        self.func_date = func_date
        self.prescription_records = prescription_records
        self.diagnosis_records = diagnosis_records
    
    
    def add_prescription_record(self, prescription_record):
        
        self.prescription_records.append(prescription_record)
    
    
    def add_diagnosis_record(self, diagnosis_record):
        
        self.diagnosis_records.append(diagnosis_record)



class LongitudeObservationalDatabase:
    
    def __init__(self, host_address, user_name, password, database_name):
        
        self.host_address = host_address
        self.user_name = user_name
        self.password = password
        self.database_name = database_name
        self.database = MySQLdb.connect(host = self.host_address, 
		                     user = self.user_name, 
		                     passwd = self.password, 
		                     db = self.database_name)
        
        # key: <type: string> drug_no
        # value : <type: string> atc_code
        self.drug_no_atc_code_hash_map = {}
        
        # key: <type: string> id + "," + str(func_date)
        # value : <type: MedicalRecord>
        self.id_func_date_medical_record_hash_map = {}
        
        # key: <type: string> id
        # value: <type: MedicalRecord>
        self.id_medical_records_hash_map = {}
        
        self.build_drug_no_atc_code_hash_map()
        self.add_prescription_records_to_id_func_date_medical_record_hash_map()
        self.add_diagnosis_records_to_id_func_date_medical_record_hash_map()
        self.build_id_medical_records_hash_map()
    
    
    def fetch_data(self, sql):
        
        # input: <type: string> sql
        # output: <type: tuple list> sql_results
        
        try:
            cursor = self.database.cursor()
            cursor.execute(sql)
            sql_results = cursor.fetchall()
            cursor.close()
            
            return sql_results
            
        except:
            print "Error! Cannot fetch data."
    
    
    def build_drug_no_atc_code_hash_map(self):
    	
        # fetch drug code raw data
        # Drug_No: drug code in NHIRD coding system
        # ATC_Code: drug code in ATC coding system
    	sql = "SELECT DISTINCT Drug_No, ATC_Code FROM Drug_Description"
    	sql_results = self.fetch_data(sql)
        
        # put keys and values to self.drug_no_atc_code_hash_map
    	for row in sql_results:
        	drug_no = row[0].strip()
        	atc_code = row[1].strip()
        	self.drug_no_atc_code_hash_map[drug_no] = atc_code


    def add_prescription_records_to_id_func_date_medical_record_hash_map(self):
        
        # fetch prescription raw data
        sql = "SELECT ID, Func_Date, Drug_No FROM Ambutory_OO2008_R20_Full"
    	sql_results = self.fetch_data(sql)
        
        # put keys and values to self.id_func_date_medical_record_hash_map
    	for row in sql_results:
            id = row[0].strip()
            func_date = row[1]
            medical_record = self.id_func_date_medical_record_hash_map.get(id + "," + str(func_date), MedicalRecord(id, func_date))
            medical_record.add_prescription_record(row[2].strip()) # drug_no
            self.id_func_date_medical_record_hash_map[id + "," + str(func_date)] = medical_record    


    def add_diagnosis_records_to_id_func_date_medical_record_hash_map(self):
        
        # fetch diagnosis raw data
        sql = "SELECT ID, Func_Date, ACode_ICD9_1, ACode_ICD9_2, ACode_ICD9_3 FROM Ambutory_CD2008_R20"
    	sql_results = self.fetch_data(sql)
        
        # put keys and values to self.id_func_date_medical_record_hash_map
    	for row in sql_results:
            id = row[0].strip()
            func_date = row[1]
            medical_record = self.id_func_date_medical_record_hash_map.get(id + "," + str(func_date), MedicalRecord(id, func_date))
            for i in range(2, 5, 1):
                medical_record.add_diagnosis_record(row[i].strip()) # acode_icd9
            self.id_func_date_medical_record_hash_map[id + "," + str(func_date)] = medical_record    
    
    
    def build_id_medical_records_hash_map(self):
        
        # extract keys and values from self.id_func_date_medical_record_hash_map
        # change keys from (id + "," + func_date) into id
        # put keys and values to self.id_func_date_medical_record_hash_map
        for id_func_date, medical_record in self.id_func_date_medical_record_hash_map.items():
            id = id_func_date.split(",")[0]
            medical_records = self.id_medical_records_hash_map.get(id, [])
            medical_records.append(medical_record)
            self.id_medical_records_hash_map[id] = medical_records


    
if __name__ == "__main__":
    print(__doc__)
    start_time = datetime.datetime.now()
    lod = LongitudeObservationalDatabase(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
    end_time = datetime.datetime.now()
    print (end_time - start_time)
    print "drug_no_atc_code_hash_map has %d items." % (len(lod.drug_no_atc_code_hash_map.items()))
    print "id_func_date_medical_record_hash_map has %d items." % (len(lod.id_func_date_medical_record_hash_map.items()))
    print "id_medical_records_hash_map has %d items." % (len(lod.id_medical_records_hash_map.items()))  



