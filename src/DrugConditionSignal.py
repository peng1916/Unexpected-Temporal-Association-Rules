#!/usr/bin/python

'''
module name: DrugConditionSignal
'''

import sys
import datetime
import operator
import IO as file_handler
from ObservationalData import LongitudeObservationalDatabase
from ObservationalData import MedicalRecord

class PatientHistory:
    
    def __init__(self, medical_records = None):   
        self.__id = medical_records[0].get_id()
        if medical_records == None:
            self.__medical_records = []
        else:
            self.__medical_records = sorted(medical_records, key = operator.attrgetter('get_func_date'))
    
    
    def get_id(self):
        return self.__id
	
    def get_medical_records(self):
        return self.__medical_records
    
    def add_medical_record(self, medical_record):
        self.__medical_records.append(medical_record)
    
    def extract_func_dates(self):
        func_dates = []
        for medical_record in self.__medical_records:
            func_dates.append(medical_record.func_date)
        func_dates = sorted(list(set(func_dates)))	
        return func_dates
    
    
    def extract_prescription_records(self): 
        prescription_records = []
        for medical_record in self.__medical_records:
            prescription_records.extend(medical_record.prescription_records)
        prescription_records = sorted(prescription_records)
        return prescription_records
	
	
    def extract_diagnosis_records(self):
        diagnosis_records = []
        for medical_record in self.__medical_records:
            diagnosis_records.extend(medical_record.diagnosis_records)
        diagnosis_records = sorted(diagnosis_records)
        return diagnosis_records
	
	
    def extract_medical_records_before_a_date(self, date):
        medical_records = []
        for medical_record in self.__medical_records:
            if medical_record.func_date < date:
                medical_records.append(medical_record)
            else:
                break
        return medical_records
    
	
    def extract_medical_records_after_a_date(self, date):
        medical_records = []
        start_index = -1
        for i in range(0, len(self.__medical_records), 1):
            if medical_record.func_date > date:
                medical_records.append(medical_record)
        return medical_records



class DrugConditionSignal:
    
    def __init__(self):
        self.__patient_histories = []


    def append_patient_history(self, patient_history):
        self.__patient_histories.append(patient_history)

    def get_patient_history(self, index):
        if len(self.__patient_histories) > 0:
            return self.__patient_histories[index]
        return None

    def number_of_patient_histories(self):
        return len(self.__patient_histories)


if __name__ == "__main__":
    print(__doc__)
    start_time = datetime.datetime.now()

    lod = LongitudeObservationalDatabase()
    drug_condition_signal = DrugConditionSignal()

    file_path = "../data/test_medical_records.csv"
    col_names, row_names, data = file_handler.read_csv(file_path, has_header = False, has_row_names = False, are_numerical_data = False)
    
    for i in range(0, (len(data) / 3), 1):
        id = data[i*3][0]
        func_date = data[i*3][1]
        prescription_records = data[(i*3+1)]
        diagnosis_records = data[(i*3+2)]
        medical_record = MedicalRecord(id, func_date, prescription_records, diagnosis_records)
        lod.add_medical_record_to_id_medical_records_hash_map(medical_record)

    for medical_records in lod.id_medical_records_hash_map.values():
        patient_history = PatientHistory(medical_records)
        drug_condition_signal.append_patient_history(patient_history)

    for i in range(0, drug_condition_signal.number_of_patient_histories(), 1):	
        patient_history = drug_condition_signal.get_patient_history(i)
        medical_records = patient_history.get_medical_records()
        print "id = %s" % (patient_history.get_id())
        print "%d medical_records" % (len(medical_records))
        #for medical_record in medical_records:
        #    medical_record.display()

    
    
    '''
    lod = LongitudeObservationalDatabase(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
    drug_condition_signal = DrugConditionSignal()
    
    for medical_records in lod.id_medical_records_hash_map.values():
        patient_history = PatientHistory(medical_records)
        drug_condition_signal.append_patient_history(patient_history)
	
    file_path = "../data/example_medical_records.csv"
    file = open(file_path, "w")
    file.close()
    
    for i in range(0, 3, 1):	
        patient_history = drug_condition_signal.get_patient_history(i)
        medical_records = patient_history.get_medical_records()
        print "id = %s" % (patient_history.get_id())
        for medical_record in medical_records:
            medical_record.write(file_path)
            medical_record.display()
    '''

    end_time = datetime.datetime.now()
    print (end_time - start_time)
	
	
