import os
import fitz  # PyMuPDF
import subprocess
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
log_file_path = os.path.abspath("pdf_scan.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),
        logging.StreamHandler()
    ]
)

# Folder containing your PDFs
pdf_folder = "./pdf_test"  # Change this to your actual folder path

# Keywords to search for
keywords = ["Trailer", "Boat", "12/31/2023"]

# Output file for matches
output_file_path = os.path.abspath("matching_files.txt")

# Compile keyword pattern for faster matching
keyword_pattern = re.compile("|".join(re.escape(k) for k in keywords), re.IGNORECASE)

# Function to extract text from the first N pages of a PDF
def extract_text_from_pdf(file_path, max_pages=5):
    try:
        doc = fitz.open(file_path)
        full_text = ""
        for page_num in range(min(max_pages, len(doc))):
            full_text += doc[page_num].get_text()
        doc.close()
        return full_text
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return ""

# Function to apply OCR using ocrmypdf
def apply_ocr(input_path, output_path):
    try:
        subprocess.run(["ocrmypdf", "--skip-text", input_path, output_path], check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"OCR failed for {input_path}: {e}")
        return False

# Function to process a single PDF file
def process_pdf(filename):
    file_path = os.path.join(pdf_folder, filename)
    text = extract_text_from_pdf(file_path, max_pages=5)

    # If no text, apply OCR
    if not text.strip():
        ocr_output_path = os.path.join(pdf_folder, f"ocr_{filename}")
        if apply_ocr(file_path, ocr_output_path):
            text = extract_text_from_pdf(ocr_output_path, max_pages=5)

    # Search for keywords
    if keyword_pattern.search(text):
        return filename
    return None

# Safe wrapper for error handling
def process_pdf_safe(filename):
    try:
        return process_pdf(filename)
    except Exception as e:
        logging.error(f"Unexpected error processing {filename}: {e}")
        return None

# Get list of PDF files
pdf_files = [f for f in os.listdir(pdf_folder) if f.lower().endswith(".pdf")]
total_files = len(pdf_files)

# Start scanning
logging.info(f"Starting scan of {total_files} PDF files...")

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=8) as executor, open(output_file_path, "a") as output_file:
    futures = {executor.submit(process_pdf_safe, filename): filename for filename in pdf_files}

    for idx, future in enumerate(as_completed(futures), start=1):
        result = future.result()
        if result:
            output_file.write(result + "\n")
            output_file.flush()
            logging.info(f"Match found: {result}")

        # Log progress every 100 files
        if idx % 100 == 0 or idx == total_files:
            logging.info(f"Processed {idx} of {total_files} files...")

logging.info("Scan complete.")
