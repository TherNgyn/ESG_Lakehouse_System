
from minio import Minio

def load_pdf_files_to_bronze_layer(pdf_paths):
    try:
        print("==================================")
        print("Starting load pdf file to bronze")
        print("==================================")
        client = Minio(
                endpoint="minio:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )
        BUCKET_NAME = "bronze"
        for pdf_path in pdf_paths:

            pdf_file = str(pdf_path).split('/')[-1]
            company_name = str(pdf_path).split('/')[-2]

            object = f"reports/{company_name}/{pdf_file}"
            client.fput_object(
                bucket_name=BUCKET_NAME,
                object_name= object,
                file_path = str(pdf_path)
            ) 
        print("==================================")
        print("Loading pdf file to bronze layer successfully")
        print("==================================")
        
    except Exception as e:
        print("Occured an error:", e)