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

    def __str__(self):
        line = ",".join(self.__prescription_records) + " | " + ",".join(self.__diagnosis_records)
        return line
    
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
        
        self.__host_address = host_address
        self.__user_name = user_name
        self.__password = password
        self.__database_name = database_name
        self.__database = None
        self.__years = [x for x in range(2004, 2009, 1)]
        self.__group_nos = [20]

        # key: <type: string> drug_no
        # value : <type: string> atc_code
        self.__drug_no_atc_code_hash_map = {}

        # key: <type: string> atc_code
        # value : <type: string> medDRA
        self.__atc_code_medDRA_hash_map = {}

        # key: <type: string> icd9cm
        # value : <type: string> umls_cid
        self.__icd9cm_umls_cid_hash_map = {}
        
        # key: <type: string> id + "," + str(func_date)
        # value : <type: MedicalRecord>
        self.__id_func_date_medical_record_hash_map = {}
        
        # key: <type: string> id
        # value: <type: MedicalRecord>
        self.__id_medical_records_hash_map = {}

        if (self.__host_address is not None) and (self.__user_name is not None) and (self.__password is not None) and (self.__database_name is not None):
            self.set_up()

    def get_id_medical_records_hash_map(self):
        return self.__id_medical_records_hash_map

    def set_up(self):

        self.__database = MySQLdb.connect(host = self.__host_address, 
		                     user = self.__user_name, 
		                     passwd = self.__password, 
		                     db = self.__database_name)
        self.build_drug_no_atc_code_hash_map()
        self.build_atc_code_medDRA_hash_map()
        self.build_icd9cm_umls_cid_hash_map()
        self.__id_func_date_medical_record_hash_map.clear() # clear self.__id_func_date_medical_record_hash_map
        self.add_prescription_records_to_id_func_date_medical_record_hash_map()
        self.add_diagnosis_records_to_id_func_date_medical_record_hash_map()
        self.build_id_medical_records_hash_map()
    
    
    def fetch_data(self, sql):
        
        # input: <type: string> sql
        # output: <type: tuple list> sql_results
        
        try:
            cursor = self.__database.cursor()
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
        
        # put keys and values to self.__drug_no_atc_code_hash_map
    	for row in sql_results:
        	drug_no = row[0].strip()
        	atc_code = row[1].strip()
        	self.__drug_no_atc_code_hash_map[drug_no] = atc_code


    def build_atc_code_medDRA_hash_map(self):
    	
        # fetch drug code raw data
        # ATC_Code: drug code in ATC coding system
        # medDRA: drug code in MedDRA coding system
    	sql = "SELECT ATC_Code, MedDRA FROM ATCCode_MedDRA"
    	sql_results = self.fetch_data(sql)
        
        # put keys and values to self.__atc_code_medDRA_hash_map
    	for row in sql_results:
        	atc_code = row[0].strip()
        	medDRA = row[1].strip()
        	self.__atc_code_medDRA_hash_map[atc_code] = medDRA


    def build_icd9cm_umls_cid_hash_map(self):
    	
        # fetch drug code raw data
        # ICD9CM: condition code in ICD9CM coding system
        # UMLSCID: condition code in UMLS coding system
    	sql = "SELECT ICD9CM, UMLSCID FROM ICD9CM_UMLSCID"
    	sql_results = self.fetch_data(sql)
        
        # put keys and values to self.__icd9cm_umls_cid_hash_map
    	for row in sql_results:
        	icd9cm = row[0].strip()
        	umls_cid = row[1].strip()
        	self.__icd9cm_umls_cid_hash_map[icd9cm] = umls_cid


    def add_prescription_records_to_id_func_date_medical_record_hash_map(self):

        for year in self.__years:
            for group_no in self.__group_nos:
                
                table_name = "Ambutory_OO" + str(year) + "_R" + str(group_no) + "_Full"
                # fetch prescription raw data
                sql = "SELECT ID, Func_Date, Drug_No FROM " + table_name
    	        sql_results = self.fetch_data(sql)
                
                # put keys and values to self.__id_func_date_medical_record_hash_map
    	        for row in sql_results:
                    drug_no = row[2].strip()
                    atc_code = self.__drug_no_atc_code_hash_map.get(drug_no, None)
                    if atc_code is None:
                        continue
                    medDRA = self.__atc_code_medDRA_hash_map.get(atc_code, None)
                    if medDRA is None:
                        continue
                    id = row[0].strip()
                    func_date = row[1]
                    key = id + "," + str(func_date)
                    medical_record = self.__id_func_date_medical_record_hash_map.get(key, MedicalRecord(id, func_date))
                    medical_record.add_prescription_record(medDRA) # medDRA
                    self.__id_func_date_medical_record_hash_map[key] = medical_record    


    def add_diagnosis_records_to_id_func_date_medical_record_hash_map(self):

        for year in self.__years:
            for group_no in self.__group_nos:
                
                table_name = "Ambutory_CD" + str(year) + "_R" + str(group_no)
                # fetch diagnosis raw data
                sql = "SELECT ID, Func_Date, ACode_ICD9_1, ACode_ICD9_2, ACode_ICD9_3 FROM " + table_name
    	        sql_results = self.fetch_data(sql)
                
                # put keys and values to self.__id_func_date_medical_record_hash_map
    	        for row in sql_results:
                    id = row[0].strip()
                    func_date = row[1]
                    key = id + "," + str(func_date)
                    medical_record = self.__id_func_date_medical_record_hash_map.get(key, MedicalRecord(id, func_date))
                    for i in range(2, 5, 1):
                        acode_icd9 = row[i].strip() # acode_icd9
                        if len(acode_icd9) > 0:
						    if acode_icd9[0] == "E" or acode_icd9[0] == "V":
						        acode_icd9 = acode_icd9[0:4] + "." + acode_icd9[4:]
						    else:
						        acode_icd9 = acode_icd9[0:3] + "." + acode_icd9[3:]
						    umls_cid = self.__icd9cm_umls_cid_hash_map.get(acode_icd9, None)
						    if umls_cid is not None:
						        medical_record.add_diagnosis_record(umls_cid)
                    #print str(medical_record)
                    self.__id_func_date_medical_record_hash_map[key] = medical_record    
    
    
    def add_medical_record_to_id_medical_records_hash_map(self, medical_record):

        id = medical_record.get_id()
        medical_records = self.__id_medical_records_hash_map.get(id, [])
        medical_records.append(medical_record)
        self.__id_medical_records_hash_map[id] = medical_records    
    
    
    def build_id_medical_records_hash_map(self):
        
        # extract keys and values from self.__id_func_date_medical_record_hash_map
        # change keys from (id + "," + func_date) into id
        # put keys and values to self.__id_func_date_medical_record_hash_map
        for id_func_date, medical_record in self.__id_func_date_medical_record_hash_map.items():
            id = id_func_date.split(",")[0]
            medical_records = self.__id_medical_records_hash_map.get(id, [])
            medical_records.append(medical_record)
            self.__id_medical_records_hash_map[id] = medical_records
    
    
        
if __name__ == "__main__":
    print(__doc__)
    start_time = datetime.datetime.now()
    lod = LongitudeObservationalDatabase(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
    end_time = datetime.datetime.now()
    print (end_time - start_time)
    print "lod.get_id_medical_records_hash_map() has %d items." % (len(lod.get_id_medical_records_hash_map().items())) 



