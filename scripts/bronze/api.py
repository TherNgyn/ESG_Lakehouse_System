import os
import json
from minio import Minio

PDF_FOLDER = "/opt/airflow/datasets/reports/vinamilk"

def get_pdf_file_paths():
    pdf_files = [
        os.path.join(PDF_FOLDER, f)
        for f in os.listdir(PDF_FOLDER)
        if f.lower().endswith(".pdf")
        ]
    return pdf_files    

from scripts.bronze.load_pdf_file import load_pdf_files_to_bronze_layer
from scripts.bronze.load_extracted_data import load_extracted_data

def load_files_to_Bronze_Layer():
    # load pdf file to bronze layer
    load_pdf_files_to_bronze_layer(pdf_paths= get_pdf_file_paths())
    
   
def load_extracted_data_to_Bronze_Layer():
    # extract data & load extracted data to bronz layer
    # với mỗi pdf -> chia nhỏ thành các file pdf nhỏ hơn 
    # extract từng file nhỏ lưu theo cấu trúc Type & Value
    load_extracted_data(get_pdf_file_paths())