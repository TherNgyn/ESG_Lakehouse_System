
from bs4 import BeautifulSoup
import requests
import os



def crawl_pdfs():
    base_url = "https://www.vinamilk.com.vn/investor/reports/sustainability"
    
    save_dir = "/opt/airflow/datasets/reports/vinamilk"
    os.makedirs(save_dir, exist_ok=True)

    headers = {
        "User-Agent": 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 Safari/537.36"
    }

    response = requests.get(base_url, headers=headers, timeout=10)  
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    
    try:
        pdf_links = []

        # thẻ div chứa các thẻ a chứa link file pdf
        div = soup.find("div", class_="grid gap-8 md:grid-cols-3")
        print("Lấy thành công thẻ div")
        # lấy tất cả thẻ a chứa trong thẻ div
        a_tags = div.find_all("a")
        print("Lấy thành công các thẻ a")

        for a in a_tags:
            pdf_link = a.get("href")
            pdf_links.append(pdf_link)
        print("lấy thành công các pdf links")
        print("Downloading pdf files............")

        for pdf_url in pdf_links:
            file_name = pdf_url.split("/")[-1]
            file_path = os.path.join(save_dir, file_name)

            r = requests.get(pdf_url,headers= headers, timeout= 20)
            with open(file_path, "wb") as f:
                f.write(r.content)
        
        print("Successfully Crawling PDF files !!!!!!!!!!!!!!!!!!!")


    except Exception as e:
        print("Occured error: ", e)
