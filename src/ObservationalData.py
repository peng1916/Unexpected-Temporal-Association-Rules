#!/usr/bin/python

'''
module name: ObservationalData
'''

import sys
import MySQLdb
import datetime


class MedicalRecord:
    

    def __init__(self, id, func_date, prescription_records = None, diagnosis_records = None):
        
        self.__id = id
        
        if type(func_date) is str:
            self.__func_date = datetime.datetime.strptime(func_date, "%Y-%m-%d")
        elif type(func_date) is datetime.date:
            self.__func_date = func_date
        elif type(func_date) is datetime.datetime:
            self.__func_date = func_date

        if prescription_records == None:
            self.__prescription_records = set()
        else:
            self.__prescription_records = set(prescription_records)

        if diagnosis_records == None:
            self.__diagnosis_records = set()
        else:
            self.__diagnosis_records = set(diagnosis_records)

    def get_id(self):
        return self.__id

    def get_func_date(self):
        return self.__func_date

    def get_prescription_records(self):
        return self.__prescription_records

    def get_diagnosis_records(self):
        return self.__diagnosis_records
    
    def add_prescription_record(self, prescription_record):
        
        self.__prescription_records.add(prescription_record)
    
    
    def add_diagnosis_record(self, diagnosis_record):
        
        self.__diagnosis_records.add(diagnosis_record)
    

    def extend_prescription_records(self, prescription_records):
        
        for prescription_record in prescription_records:
            self.__prescription_records.add(prescription_record)


    def extend_diagnosis_records(self, diagnosis_records):
        
        for diagnosis_record in diagnosis_records:
            self.__diagnosis_records.add(diagnosis_record)

    
    def write(self, file_path):
        with open(file_path, "a") as file:
            file.write(self.__id + "," + str(self.__func_date.strftime('%Y-%m-%d')) + "\n")
            file.write(",".join(sorted(self.__prescription_records)) + "\n")
            file.write(",".join(sorted(self.__diagnosis_records)) + "\n") 
    
    
    def display(self):
        print "%s" % (self.__id + "," + str(self.__func_date.strftime('%Y-%m-%d')))
        print "%s" % (",".join(sorted(self.__prescription_records)))
        print "%s" % (",".join(sorted(self.__diagnosis_records)))



class LongitudeObservationalDatabase:
    
    def __init__(self, host_address = None, user_name = None, password = None, database_name = None):
        
        self.host_address = host_address
        self.user_name = user_name
        self.password = password
        self.database_name = database_name
        self.database = None
        self.years = [x for x in range(2004, 2009, 1)]
        self.group_nos = [20]

        # key: <type: string> drug_no
        # value : <type: string> atc_code
        self.drug_no_atc_code_hash_map = {}
        
        # key: <type: string> id + "," + str(func_date)
        # value : <type: MedicalRecord>
        self.id_func_date_medical_record_hash_map = {}
        
        # key: <type: string> id
        # value: <type: MedicalRecord>
        self.id_medical_records_hash_map = {}

        if (self.host_address is not None) and (self.user_name is not None) and (self.password is not None) and (self.database_name is not None):
            self.set_up()

    def set_up(self):

        self.database = MySQLdb.connect(host = self.host_address, 
		                     user = self.user_name, 
		                     passwd = self.password, 
		                     db = self.database_name)
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

        # clear self.id_func_date_medical_record_hash_map
        self.id_func_date_medical_record_hash_map.clear()

        for year in self.years:
            for group_no in self.group_nos:
                
                table_name = "Ambutory_OO" + str(year) + "_R" + str(group_no) + "_Full"
                # fetch prescription raw data
                sql = "SELECT ID, Func_Date, Drug_No FROM " + table_name
    	        sql_results = self.fetch_data(sql)
                
                # put keys and values to self.id_func_date_medical_record_hash_map
    	        for row in sql_results:
                    drug_no = row[2].strip()
                    atc_code = self.drug_no_atc_code_hash_map.get(drug_no, None)
                    if atc_code is None:
                        continue
                    id = row[0].strip()
                    func_date = row[1]
                    key = id + "," + str(func_date)
                    medical_record = self.id_func_date_medical_record_hash_map.get(key, MedicalRecord(id, func_date))
                    medical_record.add_prescription_record(atc_code) # atc_code
                    self.id_func_date_medical_record_hash_map[key] = medical_record    


    def add_diagnosis_records_to_id_func_date_medical_record_hash_map(self):

        # clear self.id_func_date_medical_record_hash_map
        self.id_func_date_medical_record_hash_map.clear()

        for year in self.years:
            for group_no in self.group_nos:
                
                table_name = "Ambutory_CD" + str(year) + "_R" + str(group_no)
                # fetch diagnosis raw data
                sql = "SELECT ID, Func_Date, ACode_ICD9_1, ACode_ICD9_2, ACode_ICD9_3 FROM " + table_name
    	        sql_results = self.fetch_data(sql)
                
                # put keys and values to self.id_func_date_medical_record_hash_map
    	        for row in sql_results:
                    id = row[0].strip()
                    func_date = row[1]
                    key = id + "," + str(func_date)
                    medical_record = self.id_func_date_medical_record_hash_map.get(key, MedicalRecord(id, func_date))
                    for i in range(2, 5, 1):
                        acode_icd9 = row[i].strip() # acode_icd9
                        if len(acode_icd9) > 0:
                            medical_record.add_diagnosis_record(acode_icd9)
                    self.id_func_date_medical_record_hash_map[key] = medical_record    
    
    
    def add_medical_record_to_id_medical_records_hash_map(self, medical_record):

        id = medical_record.get_id()
        medical_records = self.id_medical_records_hash_map.get(id, [])
        medical_records.append(medical_record)
        self.id_medical_records_hash_map[id] = medical_records    
    
    
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



