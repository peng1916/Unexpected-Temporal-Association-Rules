#!/usr/bin/python

'''
module name: DrugConditionSignal
'''

import sys
import datetime
import IO as file_handler
from ObservationalData import LongitudeObservationalDatabase
from ObservationalData import MedicalRecord
from operator import methodcaller

class PatientHistory:
    
    def __init__(self, medical_records = None):   

        if medical_records == None:
            self.__id = None
            self.__medical_records = []
        else:
            self.__id = medical_records[0].get_id()
            self.__medical_records = sorted(medical_records, key=methodcaller('get_func_date'))
    
    def get_id(self):
        return self.__id
	
    def get_medical_records(self):
        return self.__medical_records
    
    def add_medical_record(self, medical_record):
        self.__medical_records.append(medical_record)
    
    def extract_func_dates(self):
        func_dates = []
        for medical_record in self.__medical_records:
            func_dates.append(medical_record.get_func_date())
        func_dates = sorted(list(set(func_dates)))	
        return func_dates
    
    
    def extract_prescription_records(self): 
        prescription_records = []
        for medical_record in self.__medical_records:
            prescription_records.extend(medical_record.get_prescription_records())
        prescription_records = sorted(list(set(prescription_records)))
        return prescription_records
	
	
    def extract_diagnosis_records(self):
        diagnosis_records = []
        for medical_record in self.__medical_records:
            diagnosis_records.extend(medical_record.get_diagnosis_records())
        diagnosis_records = sorted(list(set(diagnosis_records)))
        return diagnosis_records
	
    def extract_diagnosis_records_before_a_date(self, date, day_delta):
        diagnosis_records = []
        start_date = date - datetime.timedelta(days = day_delta)
        end_date = date
        for medical_record in self.__medical_records:
            if (start_date < medical_record.get_func_date()) and (medical_record.get_func_date() < end_date):
                diagnosis_records.extend(medical_record.get_diagnosis_records())
        diagnosis_records = sorted(list(set(diagnosis_records)))
        return diagnosis_records
	
    def extract_diagnosis_records_after_a_date(self, date, day_delta):
        diagnosis_records = []
        start_date = date
        end_date = date + datetime.timedelta(days = day_delta)
        for medical_record in self.__medical_records:
            if (start_date < medical_record.get_func_date()) and (medical_record.get_func_date() < end_date):
                diagnosis_records.extend(medical_record.get_diagnosis_records())
        diagnosis_records = sorted(list(set(diagnosis_records)))
        return diagnosis_records

    def detect_drug_condition_pairs(self):
        func_dates = self.extract_func_dates()
        drug_condition_pairs = []
        for medical_record in self.__medical_records:
            prescription_records = medical_record.get_prescription_records()
            if len(prescription_records) == 0:
                continue
            func_date = medical_record.get_func_date()
            diagnosis_records_before = self.extract_diagnosis_records_before_a_date(func_date, 30)
            diagnosis_records_after = self.extract_diagnosis_records_after_a_date(func_date, 30)
            diagnosis_records = []
            for diagnosis_record in diagnosis_records_after:
                if not(diagnosis_record in diagnosis_records_before):
                    diagnosis_records.append(diagnosis_record)
            if len(diagnosis_records) == 0:
                continue
            #print "%s; %s; %s" % (func_date.strftime('%Y-%m-%d'), ",".join(prescription_records), ",".join(diagnosis_records))

            # combine prescription_records and diagnosis_records
            for prescription_record in prescription_records:
                for diagnosis_record in diagnosis_records:
                    drug_condition_pairs.append(prescription_record + "," + diagnosis_record)
        #drug_condition_pairs = list(set(drug_condition_pairs)) # person base
        return drug_condition_pairs


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


    '''    
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
    
    drug_condition_pair_count_hash_map = {}
    for i in range(0, drug_condition_signal.number_of_patient_histories(), 1):	
        patient_history = drug_condition_signal.get_patient_history(i)
        medical_records = patient_history.get_medical_records()
        #print "id = %s" % (patient_history.get_id())
        #print "%d medical_records" % (len(medical_records))
        #print "%d prescriptions" % (len(patient_history.extract_prescription_records()))
        #print "%d diagnosis" % (len(patient_history.extract_diagnosis_records()))
        drug_condition_pairs = patient_history.detect_drug_condition_pairs()
        for drug_condition_pair in drug_condition_pairs:
            count = drug_condition_pair_count_hash_map.get(drug_condition_pair, 0)
            count += 1
            drug_condition_pair_count_hash_map[drug_condition_pair] = count
        #print "------------------------------------"
    
    file_path = "../data/test_drug_condition_pairs.csv"
    file = open(file_path, "w")
    file.close()

    file = open(file_path, "a")
    for drug_condition_pair, count in drug_condition_pair_count_hash_map.items():
        print "%s: %d" % (drug_condition_pair, count)
        file.write(drug_condition_pair + "," + str(count) + "\n")
    file.close()
    '''

    print "fetch data from database"
    start_time = datetime.datetime.now()    
    lod = LongitudeObservationalDatabase(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
    end_time = datetime.datetime.now()
    print (end_time - start_time)

    print "detect drug condition pairs"
    start_time = datetime.datetime.now()
    drug_condition_signal = DrugConditionSignal()    
    for medical_records in lod.id_medical_records_hash_map.values():
        patient_history = PatientHistory(medical_records)
        drug_condition_signal.append_patient_history(patient_history)

    drug_condition_pair_count_hash_map = {}
    for i in range(0, drug_condition_signal.number_of_patient_histories(), 1):	
        patient_history = drug_condition_signal.get_patient_history(i)
        medical_records = patient_history.get_medical_records()
        #print "id = %s" % (patient_history.get_id())
        #print "%d medical_records" % (len(medical_records))
        #print "%d prescriptions" % (len(patient_history.extract_prescription_records()))
        #print "%d diagnosis" % (len(patient_history.extract_diagnosis_records()))
        drug_condition_pairs = patient_history.detect_drug_condition_pairs()
        for drug_condition_pair in drug_condition_pairs:
            count = drug_condition_pair_count_hash_map.get(drug_condition_pair, 0)
            count += 1
            drug_condition_pair_count_hash_map[drug_condition_pair] = count
        #print "------------------------------------"
    end_time = datetime.datetime.now()
    print (end_time - start_time)

    print "write drug condition pairs to files"
    start_time = datetime.datetime.now()
    file_path = "../data/drug_condition_pairs.csv"
    file = open(file_path, "w")
    file.close()

    file = open(file_path, "a")
    for drug_condition_pair, count in drug_condition_pair_count_hash_map.items():
        #print "%s: %d" % (drug_condition_pair, count)
        file.write(drug_condition_pair + "," + str(count) + "\n")
    file.close()
    end_time = datetime.datetime.now()
    print (end_time - start_time)    
	
    '''
    file_path = "../data/test_medical_records.csv"
    file = open(file_path, "w")
    file.close()
    
    for i in range(0, 3, 1):	
        patient_history = drug_condition_signal.get_patient_history(i)
        medical_records = patient_history.get_medical_records()
        medical_records = sorted(medical_records, key=methodcaller('get_func_date'))
        print "id = %s" % (patient_history.get_id())
        for medical_record in medical_records:
            medical_record.write(file_path)
            medical_record.display()
    '''
	
	
