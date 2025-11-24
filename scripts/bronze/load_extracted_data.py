
from minio import Minio
from pypdf import PdfReader, PdfWriter
import os
import json
# split pdf file
def split_pdf_file(file_path):

    output_dir = "/opt/airflow/tmp"
    os.makedirs(output_dir, exist_ok=True)
    reader = PdfReader(file_path)
    name_pdf = str(str(file_path).split('/')[-1]).split('.')[0]
    pages_per_chunk = 5
    total_pages = len(reader.pages)
    chunks =[]
    for i in range(0, total_pages, pages_per_chunk):
        writer = PdfWriter()
        for j in range(i, min(i+pages_per_chunk, total_pages)):
            writer.add_page(reader.pages[j])

        chunk_file = f"{output_dir}/{name_pdf}_part{i//pages_per_chunk + 1}.pdf"
        with open(chunk_file, 'wb') as f:
            writer.write(f)
        chunks.append(chunk_file)

    return chunks



def load_data_to_minio(data_path):
    client = Minio(
                endpoint="minio:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )
    BUCKET_NAME = "bronze"
    file_name = str(data_path).split('/')[-1]
    object =f"extracted_raws/vinamilk/{file_name}"

    client.fput_object(
                bucket_name=BUCKET_NAME,
                object_name= object,
                file_path = str(data_path)
            ) 
def remove_splited_file(path):
    import shutil
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)
                  


def extracting_data(file_path):
    from unstructured.partition.pdf import partition_pdf
    
    elements = partition_pdf(
        file_path,
        languages= ['vie'],
        strategy = 'hi_res'
    )
    return elements
def summarize_extracted_data(elements_list):
    data = []
    for elements in elements_list:
        for e in elements:
            data.append({
                "type": type(e).__name__ ,
                "text": e.text
            })
    
    data_path = '/opt/airflow/tmp/extracted_data.json'

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return data_path


def load_extracted_data(pdf_file_paths):
    print("==================================")
    print("Starting extracted data to bronze")
    print("==================================")
    for file_path in pdf_file_paths:
        split_paths = split_pdf_file(file_path)
        print("==================================")
        print("split file successfully")
        print("==================================")
        total_elements = [extracting_data(path) for path in split_paths]
        print("==================================")
        print("get total elements successfully")
        data_path = summarize_extracted_data(total_elements)
        load_data_to_minio(data_path)
        remove_splited_file('/opt/airflow/tmp')

    print("==================================")
    print("Loading extracted data to bronze successfuly")
    print("==================================")