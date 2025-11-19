import os
from pathlib import Path
from unstructured.partition.pdf import partition_pdf
import subprocess

# === Thư mục chứa file python ===
CURRENT_DIR = Path(__file__).resolve().parent

# === Thư mục gốc chứa requirement ===
BASE_DIR = CURRENT_DIR.parent

# === Đường dẫn Poppler & Tesseract ===
POPLER_PATH = BASE_DIR / "requirement" / "poppler-25.07.0" / "Library" / "bin"
TESSERACT_PATH = BASE_DIR / "requirement" / "Tesseract-OCR"
TESSDATA_PATH = TESSERACT_PATH / "tessdata"

# === Set PATH ===
os.environ["PATH"] += os.pathsep + str(POPLER_PATH)
os.environ["PATH"] += os.pathsep + str(TESSERACT_PATH)

# === Set TESSDATA_PREFIX ===
os.environ["TESSDATA_PREFIX"] = str(TESSDATA_PATH)

# === Kiểm tra Poppler & Tesseract ===
subprocess.run(["pdfinfo", "-v"])
subprocess.run(["tesseract", "-v"])

# === Đường dẫn PDF tương đối (ví dụ datasets nằm cùng cấp requirement) ===
pdf_path = BASE_DIR / "datasets" / "test.pdf"

print("PDF exists:", pdf_path.exists())
print("PDF path:", pdf_path)

# === Extract PDF ===
elements = partition_pdf(
    filename=str(pdf_path),
    strategy="hi_res",
    languages=["vie"]
)

print(f"Số elements trích xuất: {len(elements)}")
for el in elements:
    print(type(el), el.text)
