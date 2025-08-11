import os
import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from googletrans import Translator
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CFETariffScraperSimplified:
    def __init__(self, output_dir, headless=False):
        # Define all fare types and their URLs
        self.fare_urls = {
            "GDMTO": "https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCREIndustria/Tarifas/GranDemandaMTO.aspx",
            "GDMTH": "https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCREIndustria/Tarifas/GranDemandaMTH.aspx", 
            "DIST": "https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCREIndustria/Tarifas/DemandaIndustrialSub.aspx",
            "DIT": "https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCREIndustria/Tarifas/DemandaIndustrialTran.aspx"
        }
        
        self.output_dir = output_dir
        self.driver = None
        self.wait = None
        self.translator = Translator()
        self.setup_driver(headless)
        
        # Create output directories
        os.makedirs(output_dir, exist_ok=True)
        self.extraction_dir = os.path.join(output_dir, "extraction")
        os.makedirs(self.extraction_dir, exist_ok=True)
        
        # Initialize JSON files (main consolidated files)
        self.original_data_file = os.path.join(output_dir, "cfe_tariff_data_spanish.json")
        self.translated_data_file = os.path.join(output_dir, "cfe_tariff_data_english.json")
        
        # Initialize failure tracking
        self.failed_extractions_file = os.path.join(output_dir, "failed_extractions.json")
        self.failed_extractions = self.load_existing_data(self.failed_extractions_file)
        
        # Load existing data
        self.original_data = self.load_existing_data(self.original_data_file)
        self.translated_data = self.load_existing_data(self.translated_data_file)
        
    def load_existing_data(self, filepath):
        """Load existing data from JSON file"""
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            logger.warning(f"Could not load existing data from {filepath}: {e}")
            return []
    
    def save_json_data(self, data, filepath):
        """Save data to JSON file"""
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to: {filepath}")
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {e}")
    
    def create_safe_filename(self, name):
        """Create safe filename by removing/replacing problematic characters"""
        safe_name = name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        safe_name = safe_name.replace(":", "_").replace("*", "_").replace("?", "_")
        safe_name = safe_name.replace('"', "_").replace("<", "_").replace(">", "_")
        safe_name = safe_name.replace("|", "_").replace(".", "_")
        return safe_name
    
    def get_region_municipality_path(self, region_name, municipality_name):
        """Get the folder path for a specific region and municipality"""
        safe_region = self.create_safe_filename(region_name)
        safe_municipality = self.create_safe_filename(municipality_name)
        
        region_path = os.path.join(self.extraction_dir, safe_region)
        municipality_path = os.path.join(region_path, safe_municipality)
        
        # Create directories if they don't exist
        os.makedirs(municipality_path, exist_ok=True)
        
        return municipality_path
    
    def track_failure(self, fare_type, region_name, municipality_name, division_name, year, month, error_msg):
        """Track failed extractions"""
        failure_record = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "fare_type": fare_type,
            "region": region_name,
            "municipality": municipality_name,
            "division": division_name,
            "year": year,
            "month": month,
            "error": str(error_msg)
        }
        
        self.failed_extractions.append(failure_record)
        self.save_json_data(self.failed_extractions, self.failed_extractions_file)
        logger.error(f"Failure tracked: {fare_type} - {region_name}/{municipality_name}/{division_name} - {error_msg}")
    
    def translate_text(self, text, dest='en'):
        """Translate text using Google Translate"""
        try:
            if not text or text.strip() == "":
                return text
            translated = self.translator.translate(text, dest=dest)
            return translated.text
        except Exception as e:
            logger.warning(f"Translation failed for '{text}': {e}")
            return text
    
    def setup_driver(self, headless):
        """Initialize Chrome driver with options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)
    
    def wait_for_page_load(self, timeout=10):
        """Wait for page to fully load after dropdown selection"""
        time.sleep(2)
        try:
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            time.sleep(1)
        except TimeoutException:
            logger.warning("Page load timeout, continuing...")
    
    def select_dropdown_option(self, dropdown_id, value, value_type="value"):
        """Select option from dropdown and wait for page refresh"""
        try:
            dropdown_element = self.wait.until(EC.presence_of_element_located((By.ID, dropdown_id)))
            dropdown = Select(dropdown_element)
            
            if value_type == "value":
                dropdown.select_by_value(str(value))
            elif value_type == "text":
                dropdown.select_by_visible_text(value)
            
            logger.info(f"Selected {value} from {dropdown_id}")
            self.wait_for_page_load()
            return True
            
        except (TimeoutException, NoSuchElementException) as e:
            logger.error(f"Error selecting {value} from {dropdown_id}: {e}")
            return False
    
    def get_available_options(self, dropdown_id):
        """Get all available options from a dropdown"""
        try:
            dropdown_element = self.wait.until(EC.presence_of_element_located((By.ID, dropdown_id)))
            dropdown = Select(dropdown_element)
            options = []
            
            for option in dropdown.options:
                value = option.get_attribute('value')
                text = option.text.strip()
                if value and value != "0" and text and "Seleccione" not in text and "Select" not in text:
                    options.append({"value": value, "text": text})
            
            return options
        except (TimeoutException, NoSuchElementException) as e:
            logger.error(f"Error getting options from {dropdown_id}: {e}")
            return []
    
    def extract_clean_text(self, element):
        """Extract clean text from element, handling nested font tags"""
        try:
            text = element.text.strip()
            if not text:
                text = element.get_attribute('textContent').strip()
            return text
        except:
            return ""
    
    def extract_table_data_simplified(self, fare_type, region_name, municipality_name, division_name, year, month):
        """Simplified table extraction - only fare, post, units, and tariff value"""
        try:
            # Find the table
            table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered")))
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            if len(rows) < 2:
                logger.warning("No data rows found in table")
                return []
            
            table_data = []
            
            # Process data rows
            for i, row in enumerate(rows[1:], 1):
                th_cells = row.find_elements(By.TAG_NAME, "th")
                td_cells = row.find_elements(By.TAG_NAME, "td")
                
                # We only need the last 3 td cells (post, units, value)
                if len(td_cells) >= 3:
                    # Get the last 3 td cells
                    post = self.extract_clean_text(td_cells[-3])
                    units = self.extract_clean_text(td_cells[-2])
                    tariff_value = self.extract_clean_text(td_cells[-1]).replace(",", "")
                    
                    row_data = {
                        "id": f"{region_name}_{municipality_name}_{division_name}_{year}_{month}_{i}",
                        "region": region_name,
                        "municipality": municipality_name,
                        "division": division_name,
                        "year": str(year),
                        "month": month,
                        "month_name": self.get_month_name(month),
                        "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "fare": fare_type,
                        "post": post,
                        "units": units,
                        "tariff_value": tariff_value
                    }
                    
                    table_data.append(row_data)
                    logger.info(f"Row {i}: Fare={fare_type}, Post={post}, Units={units}, Value={tariff_value}")
            
            logger.info(f"Successfully extracted {len(table_data)} rows of data")
            return table_data
            
        except Exception as e:
            logger.error(f"Error extracting table data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_month_name(self, month_num):
        """Get month name in Spanish"""
        month_names = {
            1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
            5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO", 
            9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"
        }
        return month_names.get(month_num, str(month_num))
    
    def translate_data_record(self, record):
        """Translate specific fields in a record"""
        translated_record = record.copy()
        
        # Translate specific fields
        fields_to_translate = [
            "region", "municipality", "division", "post", "units", "month_name"
        ]
        
        for field in fields_to_translate:
            if field in translated_record and translated_record[field]:
                translated_record[field] = self.translate_text(translated_record[field])
        
        return translated_record
    
    def save_individual_files(self, data, fare_type, region_name, municipality_name, division_name, year, month):
        """Save data to individual files in the nested folder structure"""
        if not data:
            return
        
        try:
            municipality_path = self.get_region_municipality_path(region_name, municipality_name)
            
            # Create filename with fare_type, division, year, month info
            safe_division = self.create_safe_filename(division_name)
            filename_base = f"{fare_type}_{safe_division}_{year}_{month:02d}"
            
            # Save Spanish version
            spanish_file = os.path.join(municipality_path, f"{filename_base}_spanish.json")
            self.save_json_data(data, spanish_file)
            
            # Translate and save English version
            translated_data = [self.translate_data_record(record) for record in data]
            english_file = os.path.join(municipality_path, f"{filename_base}_english.json")
            self.save_json_data(translated_data, english_file)
            
            logger.info(f"Individual files saved for {fare_type}/{region_name}/{municipality_name}/{division_name}")
            
        except Exception as e:
            logger.error(f"Error saving individual files: {e}")
    
    def append_and_save_data(self, new_data, fare_type, region_name, municipality_name, division_name, year, month):
        """Append new data to existing collections and save both consolidated and individual files"""
        if not new_data:
            return
        
        # Save individual files first
        self.save_individual_files(new_data, fare_type, region_name, municipality_name, division_name, year, month)
        
        # Append to consolidated data
        self.original_data.extend(new_data)
        self.save_json_data(self.original_data, self.original_data_file)
        
        # Translate and append to consolidated translated data
        translated_new_data = [self.translate_data_record(record) for record in new_data]
        self.translated_data.extend(translated_new_data)
        self.save_json_data(self.translated_data, self.translated_data_file)
        
        logger.info(f"Appended {len(new_data)} new records. Total records: {len(self.original_data)}")
    
    def scrape_all_data(self):
        """Main scraping function for all fare types"""
        try:
            # Define periods to scrape
            periods = []
            
            # 2024: September to December
            for month in range(9, 13):
                periods.append({"year": "2024", "month": month})
            
            # 2025: All available months
            for month in range(1, 13):
                periods.append({"year": "2025", "month": month})
            
            # Iterate through all fare types
            for fare_type, base_url in self.fare_urls.items():
                logger.info(f"Processing fare type: {fare_type}")
                
                for period in periods:
                    year = period["year"]
                    month = period["month"]
                    
                    logger.info(f"Processing {fare_type} - {year}-{month:02d}")
                    
                    try:
                        # Navigate to specific fare URL
                        self.driver.get(base_url)
                        self.wait_for_page_load()
                        
                        # Select year
                        if not self.select_dropdown_option("ContentPlaceHolder1_Fecha_ddAnio", year):
                            continue
                        
                        # Select month
                        if not self.select_dropdown_option("ContentPlaceHolder1_MesVerano3_ddMesConsulta", month):
                            continue
                        
                        # Get regions
                        regions = self.get_available_options("ContentPlaceHolder1_EdoMpoDiv_ddEstado")
                        logger.info(f"Found {len(regions)} regions for {fare_type}")
                        
                        for region in regions:
                            region_value = region["value"]
                            region_name = region["text"]
                            
                            logger.info(f"Processing {fare_type} - region: {region_name}")
                            
                            try:
                                # Select region
                                if not self.select_dropdown_option("ContentPlaceHolder1_EdoMpoDiv_ddEstado", region_value):
                                    self.track_failure(fare_type, region_name, "N/A", "N/A", year, month, "Failed to select region")
                                    continue
                                
                                # Get municipalities
                                municipalities = self.get_available_options("ContentPlaceHolder1_EdoMpoDiv_ddMunicipio")
                                
                                for municipality in municipalities:
                                    municipality_value = municipality["value"]
                                    municipality_name = municipality["text"]
                                    
                                    logger.info(f"Processing {fare_type} - municipality: {municipality_name}")
                                    
                                    try:
                                        # Select municipality
                                        if not self.select_dropdown_option("ContentPlaceHolder1_EdoMpoDiv_ddMunicipio", municipality_value):
                                            self.track_failure(fare_type, region_name, municipality_name, "N/A", year, month, "Failed to select municipality")
                                            continue
                                        
                                        # Get divisions
                                        divisions = self.get_available_options("ContentPlaceHolder1_EdoMpoDiv_ddDivision")
                                        
                                        if not divisions:
                                            self.track_failure(fare_type, region_name, municipality_name, "N/A", year, month, "No divisions available")
                                            continue
                                        
                                        for division in divisions:
                                            division_value = division["value"]
                                            division_name = division["text"]
                                            
                                            logger.info(f"Processing {fare_type} - division: {division_name}")
                                            
                                            try:
                                                # Select division
                                                if not self.select_dropdown_option("ContentPlaceHolder1_EdoMpoDiv_ddDivision", division_value):
                                                    self.track_failure(fare_type, region_name, municipality_name, division_name, year, month, "Failed to select division")
                                                    continue
                                                
                                                # Extract table data with simplified logic
                                                table_data = self.extract_table_data_simplified(
                                                    fare_type, region_name, municipality_name, division_name, year, month
                                                )
                                                
                                                if table_data:
                                                    self.append_and_save_data(table_data, fare_type, region_name, municipality_name, division_name, year, month)
                                                    logger.info(f"Successfully extracted and saved data for {fare_type}/{region_name}/{municipality_name}/{division_name}")
                                                else:
                                                    self.track_failure(fare_type, region_name, municipality_name, division_name, year, month, "No table data extracted")
                                                    logger.warning(f"No data found for {fare_type}/{region_name}/{municipality_name}/{division_name}")
                                                
                                            except Exception as e:
                                                self.track_failure(fare_type, region_name, municipality_name, division_name, year, month, str(e))
                                                logger.error(f"Error processing {fare_type} division {division_name}: {e}")
                                                continue
                                            
                                            # Go back to municipality selection
                                            self.select_dropdown_option("ContentPlaceHolder1_EdoMpoDiv_ddMunicipio", municipality_value)
                                    
                                    except Exception as e:
                                        self.track_failure(fare_type, region_name, municipality_name, "N/A", year, month, str(e))
                                        logger.error(f"Error processing {fare_type} municipality {municipality_name}: {e}")
                                        continue
                                    
                                    # Go back to region selection
                                    self.select_dropdown_option("ContentPlaceHolder1_EdoMpoDiv_ddEstado", region_value)
                            
                            except Exception as e:
                                self.track_failure(fare_type, region_name, "N/A", "N/A", year, month, str(e))
                                logger.error(f"Error processing {fare_type} region {region_name}: {e}")
                                continue
                    
                    except Exception as e:
                        logger.error(f"Error processing {fare_type} period {year}-{month}: {e}")
                        continue
            
            logger.info(f"Scraping completed. Total records: Original={len(self.original_data)}, Translated={len(self.translated_data)}")
            logger.info(f"Total failures tracked: {len(self.failed_extractions)}")
            
        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
        except Exception as e:
            logger.error(f"Error in main scraping function: {e}")
        finally:
            try:
                self.driver.quit()
            except:
                pass

def main():
    """Run the scraper"""
    output_directory = r"C:\Users\Jash\Downloads\clem_trans_plant_details"
    scraper = CFETariffScraperSimplified(output_directory, headless=False)
    scraper.scrape_all_data()

if __name__ == "__main__":
    main()
