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
    
    def __init__(self, id, medical_records):

        self.__id = id
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


class UnexpectedDrugConditionSignal:
    
    def __init__(self):
        self.__patient_histories = []
        self.__number_of_drug_condition_pair = 0
        self.__drug_condition_pair_count_hash_map = {}
        self.__drug_count_hash_map = {}
        self.__condition_count_hash_map = {}
        self.__drug_condition_pair_leverage_hash_map = {}

    def get_drug_condition_pair_count_hash_map(self):
        return self.__drug_condition_pair_count_hash_map

    def get_drug_count_hash_map(self):
        return self.__drug_count_hash_map

    def get_condition_count_hash_map(self):
        return self.__condition_count_hash_map

    def get_drug_condition_pair_leverage_hash_map(self):
        return self.__drug_condition_pair_leverage_hash_map

    def build_patient_histories(self, id_medical_records_hash_map):
        del self.__patient_histories[:] # delete elements in self.__patient_histories
        for id, medical_records in id_medical_records_hash_map.items():
            patient_history = PatientHistory(id, medical_records)
            self.__patient_histories.append(patient_history)

    def append_patient_history(self, patient_history):
        self.__patient_histories.append(patient_history)

    def get_patient_history(self, index):
        if len(self.__patient_histories) > 0:
            return self.__patient_histories[index]
        return None

    def number_of_patient_histories(self):
        return len(self.__patient_histories)

    def build_count_hash_maps(self):        
        for patient_history in self.__patient_histories:            
            drug_condition_pairs = patient_history.detect_drug_condition_pairs()
            for drug_condition_pair in drug_condition_pairs:
                self.__number_of_drug_condition_pair += 1
                drug = drug_condition_pair.split(",")[0]
                condition = drug_condition_pair.split(",")[1]
                # update self.__drug_condition_pair_count_hash_map
                count = self.__drug_condition_pair_count_hash_map.get(drug_condition_pair, 0)
                count += 1
                self.__drug_condition_pair_count_hash_map[drug_condition_pair] = count
                # update self.__drug_count_hash_map
                count = self.__drug_count_hash_map.get(drug, 0)
                count += 1
                self.__drug_count_hash_map[drug] = count
                # update self.__condition_count_hash_map
                count = self.__condition_count_hash_map.get(condition, 0)
                count += 1
                self.__condition_count_hash_map[condition] = count

    def build_leverage_hash_map(self):
        number_of_drug_condition_pair = float(self.__number_of_drug_condition_pair)
        for drug_condition_pair, drug_condition_pair_count in self.__drug_condition_pair_count_hash_map.items():
            drug = drug_condition_pair.split(",")[0]
            condition = drug_condition_pair.split(",")[1]
            drug_condition_pair_count = float(drug_condition_pair_count)
            drug_count = float(self.__drug_count_hash_map.get(drug, 0))
            condition_count = float(self.__condition_count_hash_map.get(condition, 0))
            leverage = (drug_condition_pair_count/number_of_drug_condition_pair) - (drug_count/number_of_drug_condition_pair)*(condition_count/number_of_drug_condition_pair)
            self.__drug_condition_pair_leverage_hash_map[drug_condition_pair] = leverage
            


def write_hash_map_to_file(file_path, hash_map):
    file = open(file_path, "w")
    file.close()
    
    file = open(file_path, "a")
    for key, value in hash_map.items():
        file.write(str(key) + "," + str(value) + "\n")
    file.close()


if __name__ == "__main__":
    print(__doc__)
    
    print "fetch data from database"
    start_time = datetime.datetime.now()    
    lod = LongitudeObservationalDatabase(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))
    end_time = datetime.datetime.now()
    print (end_time - start_time)

    print "detect drug condition pairs"
    start_time = datetime.datetime.now()
    udcs = UnexpectedDrugConditionSignal()    
    udcs.build_patient_histories(lod.get_id_medical_records_hash_map())
    udcs.build_count_hash_maps()
    udcs.build_leverage_hash_map()
    end_time = datetime.datetime.now()
    print (end_time - start_time)

    print "write results to files"
    start_time = datetime.datetime.now()
    write_hash_map_to_file(file_path = "../data/unexpected_drug_condition_pair_count_table.csv", 
                           hash_map = udcs.get_drug_condition_pair_count_hash_map())
    write_hash_map_to_file(file_path = "../data/unexpected_drug_count_table.csv", 
                           hash_map = udcs.get_drug_count_hash_map())
    write_hash_map_to_file(file_path = "../data/unexpected_condition_count_table.csv", 
                           hash_map = udcs.get_condition_count_hash_map())
    write_hash_map_to_file(file_path = "../data/unexpected_drug_condition_pair_leverage_table.csv", 
                           hash_map = udcs.get_drug_condition_pair_leverage_hash_map())
    end_time = datetime.datetime.now()
    print (end_time - start_time)
    