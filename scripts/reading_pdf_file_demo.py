import os
from pathlib import Path
from unstructured.partition.pdf import partition_pdf
import subprocess

# Thêm Poppler vào PATH tạm thời trong venv
os.environ["PATH"] += r";D:\Projects for CV\ESG_Lakehouse_System\requirement\poppler-25.07.0\Library\bin"
os.environ["PATH"] += r";D:\Projects for CV\ESG_Lakehouse_System\requirement\Tesseract-OCR"
os.environ["TESSDATA_PREFIX"] = r"D:\Projects for CV\ESG_Lakehouse_System\venv\Tesseract-OCR\tessdata"

# Kiểm tra thư viện required
subprocess.run(["pdfinfo", "-v"])
subprocess.run(["tesseract", "-v"])


# PDF path tuyệt đối dựa trên script
pdf_path = Path(__file__).parent / "../datasets/test.pdf"
print("PDF exists:", pdf_path.exists())

print(str(pdf_path))
elements = partition_pdf(
    filename= str(pdf_path),
    strategy="hi_res",
    languages=["vie"]
)

print(f"Số elements trích xuất: {len(elements)}")
for el in elements:
    print(type(el), el.text)
