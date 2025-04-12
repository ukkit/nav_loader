"""
AMFI NAV Loader
Version: 1.0.0

Copyright 2025

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import logging
import os
import time
import random
import psutil
import pandas as pd
import requests
import chardet
import sys
import mysql.connector
from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple
import holidays

class NAVLoader:
    # Configuration constants
    MIN_DELAY = 2  # Minimum delay between requests in seconds
    MAX_DELAY = 5  # Maximum delay between requests in seconds
    MAX_RETRIES = 3  # Maximum number of retries for failed requests
    RETRY_DELAY = 10  # Delay between retries in seconds
    NAVALL_BASE_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}&todt={}"
    
    def __init__(self, db_config: dict = None):
        """
        Initialize the NAVLoader with database configuration.
        
        Args:
            db_config (dict): Database configuration dictionary
        """
        self.db_config = db_config or {
            'host': os.getenv('MYSQL_HOST', 'mysqldb'),
            'user': os.getenv('MYSQL_USER', 'bob'),
            'password': os.getenv('MYSQL_PASSWORD', 'marley'),
            'database': os.getenv('MYSQL_DATABASE', 'dont_worry'),
            'port': int(os.getenv('MYSQL_PORT', 3306))
        }
        
        # Initialize Indian holidays
        self.indian_holidays = holidays.India()
        
        # Configure logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure logging for the NAVLoader."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/nav_loader.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """Get a database connection."""
        return mysql.connector.connect(**self.db_config)
    
    def get_random_delay(self) -> float:
        """Generate a random delay between requests."""
        return random.uniform(self.MIN_DELAY, self.MAX_DELAY)
    
    def get_latest_business_day(self, date: datetime) -> datetime:
        """Get the latest business day before the given date."""
        while date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            date -= timedelta(days=1)
        return date
    
    def download_nav_file_for_date(self, date: datetime) -> str:
        """
        Download NAV file for a specific date with rate limiting and retries.
        
        Args:
            date (datetime): The date for which to download NAV data
            
        Returns:
            str: Path to the downloaded file
            
        Raises:
            Exception: If download fails after all retries
        """
        nav_date = date.strftime('%d-%b-%Y')
        url = self.NAVALL_BASE_URL.format(nav_date, nav_date)
        
        for attempt in range(self.MAX_RETRIES):
            try:
                time.sleep(self.get_random_delay())
                response = requests.get(url)
                if response.status_code == 200 and any(char.isdigit() for char in response.text):
                    file_path = f"data/navall_{date.strftime('%Y-%m-%d')}.txt"
                    os.makedirs("data", exist_ok=True)
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    return file_path
                else:
                    error_msg = f"Download failed for {date.strftime('%Y-%m-%d')}. Status code: {response.status_code}"
                    if attempt < self.MAX_RETRIES - 1:
                        self.logger.warning(f"{error_msg}. Retrying in {self.RETRY_DELAY} seconds...")
                        time.sleep(self.RETRY_DELAY)
                    else:
                        self.logger.error(f"{error_msg}. Response content: {response.text[:200]}...")
                        raise Exception(error_msg)
            except requests.exceptions.RequestException as e:
                error_msg = f"Network error for {date.strftime('%Y-%m-%d')}: {str(e)}"
                if attempt < self.MAX_RETRIES - 1:
                    self.logger.warning(f"{error_msg}. Retrying in {self.RETRY_DELAY} seconds...")
                    time.sleep(self.RETRY_DELAY)
                else:
                    self.logger.error(error_msg)
                    raise Exception(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error for {date.strftime('%Y-%m-%d')}: {str(e)}"
                if attempt < self.MAX_RETRIES - 1:
                    self.logger.warning(f"{error_msg}. Retrying in {self.RETRY_DELAY} seconds...")
                    time.sleep(self.RETRY_DELAY)
                else:
                    self.logger.error(error_msg)
                    raise Exception(error_msg)
    
    def parse_nav_file(self, file_path: str) -> pd.DataFrame:
        """
        Parse NAV data from a file.
        
        Args:
            file_path (str): Path to the NAV file
            
        Returns:
            pd.DataFrame: Parsed NAV data
        """
        with open(file_path, 'rb') as raw:
            raw_data = raw.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding'] or 'latin1'

        lines = raw_data.decode(encoding, errors='replace').splitlines()

        data = []
        scheme_type = scheme_category = scheme_sub_category = fund_structure = ""
        fund_house = ""

        for line in lines:
            line = line.strip()
            if not line:
                continue
            if ';' not in line:
                if line.startswith("Open Ended") or line.startswith("Close Ended"):
                    scheme_type = line
                    scheme_category = scheme_sub_category = ""
                elif "Fund" in line:
                    fund_house = line
                continue
            parts = line.split(';')
            if len(parts) == 8:
                scheme_code, scheme_name, isin_growth, isin_reinv, nav, repurchase, sale, nav_date = parts
                data.append([
                    scheme_type,
                    scheme_category,
                    scheme_sub_category,
                    scheme_code,
                    isin_growth,
                    isin_reinv,
                    scheme_name,
                    nav,
                    nav_date,
                    fund_house
                ])

        return pd.DataFrame.from_records(data, columns=[
            "Scheme Type",
            "Scheme Category",
            "Scheme Sub-Category",
            "Scheme Code",
            "ISIN Div Payout/ISIN Growth",
            "ISIN Div Reinvestment",
            "Scheme Name",
            "Net Asset Value",
            "Date",
            "Fund Structure"
        ])
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean the NAV data before insertion.
        
        Args:
            df (pd.DataFrame): Input DataFrame containing NAV data
            
        Returns:
            pd.DataFrame: Cleaned and validated DataFrame
        """
        if df.empty:
            raise ValueError("Empty DataFrame received")
        
        # Create a copy of the DataFrame to avoid SettingWithCopyWarning
        df = df.copy()
        
        # Check if first row contains header values
        if df.iloc[0, 0] == 'Scheme Code':
            df = df.iloc[1:].reset_index(drop=True)
            self.logger.warning("Header row detected and removed")
        
        # Validate required columns
        required_columns = ['Scheme Code', 'Scheme Name', 'Net Asset Value', 'Date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Clean and validate data
        try:
            # Convert NAV values to float, handling any invalid values
            df.loc[:, 'Net Asset Value'] = pd.to_numeric(df['Net Asset Value'], errors='coerce')
            
            # Remove rows with invalid NAV values
            invalid_nav_rows = df['Net Asset Value'].isna()
            if invalid_nav_rows.any():
                self.logger.warning(f"Removing {invalid_nav_rows.sum()} rows with invalid NAV values")
                df = df[~invalid_nav_rows]
            
            # Convert dates to datetime
            df.loc[:, 'Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y', errors='coerce')
            
            # Remove rows with invalid dates
            invalid_date_rows = df['Date'].isna()
            if invalid_date_rows.any():
                self.logger.warning(f"Removing {invalid_date_rows.sum()} rows with invalid dates")
                df = df[~invalid_date_rows]
            
            # Clean scheme codes and names
            df.loc[:, 'Scheme Code'] = df['Scheme Code'].str.strip()
            df.loc[:, 'Scheme Name'] = df['Scheme Name'].str.strip()
            
            # Remove any empty rows
            df = df.dropna(how='all')
            
            if df.empty:
                raise ValueError("No valid data remaining after cleaning")
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error during data validation: {str(e)}")
            raise ValueError(f"Data validation failed: {str(e)}")
    
    def insert_nav(self, df: pd.DataFrame) -> None:
        """
        Insert NAV data into the database.
        
        Args:
            df (pd.DataFrame): DataFrame containing NAV data to insert
        """
        df = self.validate_data(df)
        
        # Process data in chunks to manage memory
        chunk_size = 1000
        total_rows = len(df)
        processed_rows = 0
        failed_rows = 0
        failed_schemes = set()
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                
                # Prepare data for insertion
                values = []
                for _, row in chunk.iterrows():
                    try:
                        values.append((
                            row['Scheme Type'],
                            row['Scheme Category'],
                            row['Scheme Sub-Category'],
                            row['Scheme Code'],
                            row['ISIN Div Payout/ISIN Growth'],
                            row['ISIN Div Reinvestment'],
                            row['Scheme Name'],
                            float(row['Net Asset Value']),
                            row['Date'].date(),
                            row['Fund Structure']
                        ))
                    except Exception as e:
                        failed_rows += 1
                        failed_schemes.add(row['Scheme Code'])
                        self.logger.warning(f"Skipping row due to invalid data: {row.to_dict()}. Error: {str(e)}")
                        continue
                
                if not values:
                    self.logger.warning("No valid rows to insert in this chunk")
                    continue
                
                try:
                    # Insert data
                    query = """
                        INSERT INTO nav_data 
                        (scheme_type, scheme_category, scheme_sub_category, scheme_code, 
                         isin_growth, isin_reinv, scheme_name, nav, nav_date, fund_structure)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        nav = VALUES(nav)
                    """
                    cursor.executemany(query, values)
                    conn.commit()
                    
                    processed_rows += len(values)
                    self.logger.info(f"Processed {processed_rows}/{total_rows} rows")
                    
                except mysql.connector.Error as e:
                    conn.rollback()
                    error_msg = f"Database error: {e.msg} (Error code: {e.errno})"
                    self.logger.error(error_msg)
                    # Log the first few failed values for debugging
                    for v in values[:5]:
                        self.logger.error(f"Failed value example: {v}")
                    raise Exception(error_msg)
                
        except Exception as e:
            conn.rollback()
            error_msg = f"Error inserting data: {str(e)}"
            self.logger.error(error_msg)
            raise
        finally:
            cursor.close()
            conn.close()
            
            # Log summary of failed operations
            if failed_rows > 0:
                self.logger.error(f"Failed to process {failed_rows} rows")
                self.logger.error(f"Affected schemes: {', '.join(sorted(failed_schemes))}")
            
            if processed_rows < total_rows:
                self.logger.warning(f"Only processed {processed_rows} out of {total_rows} rows")
    
    def get_latest_nav_date(self) -> Optional[datetime]:
        """Get the latest NAV date from the database."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT MAX(nav_date) FROM nav_data")
            result = cursor.fetchone()
            return result[0] if result[0] else None
        finally:
            cursor.close()
            conn.close()
    
    def get_earliest_nav_date(self) -> Optional[datetime]:
        """Get the earliest NAV date from the database."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT MIN(nav_date) FROM nav_data")
            result = cursor.fetchone()
            return result[0] if result[0] else None
        finally:
            cursor.close()
            conn.close()
    
    def is_business_day(self, date: datetime) -> bool:
        """
        Check if a date is a business day (not weekend or holiday).
        
        Args:
            date (datetime): Date to check
            
        Returns:
            bool: True if it's a business day, False otherwise
        """
        # Convert to date object if it's a datetime
        if isinstance(date, datetime):
            date = date.date()
            
        # Check if it's a weekend
        if date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            return False
            
        # Check if it's a holiday
        if date in self.indian_holidays:
            self.logger.info(f"{date} is a holiday: {self.indian_holidays.get(date)}")
            return False
            
        return True
    
    def bulk_download_past_years(self, years: int = 15) -> List[str]:
        """
        Download NAV data for the specified number of past years.
        Starts from the earliest date in the database and goes back.
        
        Args:
            years (int): Number of past years to download data for
            
        Returns:
            List[str]: List of downloaded file paths
        """
        # Get the earliest date from database
        earliest_date = self.get_earliest_nav_date()
        if earliest_date is None:
            self.logger.info("No existing data in database. Starting from today.")
            end_date = datetime.now()
        else:
            self.logger.info(f"Found existing data in database. Starting from {earliest_date}")
            end_date = earliest_date - timedelta(days=1)
        
        # Calculate start date
        start_date = end_date - timedelta(days=years*365)
        
        self.logger.info(f"Processing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        downloaded_files = []
        current_date = end_date
        
        while current_date >= start_date:
            if self.is_business_day(current_date):
                file_path = f"data/navall_{current_date.strftime('%Y-%m-%d')}.txt"
                if not os.path.exists(file_path):
                    try:
                        self.logger.info(f"Downloading for {current_date.strftime('%Y-%m-%d')}...")
                        self.download_nav_file_for_date(current_date)
                        downloaded_files.append(file_path)
                    except Exception as e:
                        self.logger.error(f"Failed for {current_date.strftime('%Y-%m-%d')}: {e}")
                else:
                    self.logger.info(f"File already exists: {file_path}")
                    downloaded_files.append(file_path)
            
            current_date -= timedelta(days=1)
        
        return downloaded_files
    
    def bulk_download_past_months(self, months: int = 3, start_date: datetime = None, end_date: datetime = None) -> List[str]:
        """
        Download NAV data for the specified number of past months.
        
        Args:
            months (int): Number of past months to download data for
            start_date (datetime): Optional start date
            end_date (datetime): Optional end date
            
        Returns:
            List[str]: List of downloaded file paths
        """
        if not end_date:
            end_date = self.get_latest_business_day(datetime.now())
        if not start_date:
            start_date = end_date - timedelta(days=months*30)
        
        downloaded_files = []
        os.makedirs("data", exist_ok=True)
        
        current_date = end_date
        while current_date >= start_date:
            if self.is_business_day(current_date):
                file_path = f"data/navall_{current_date.strftime('%Y-%m-%d')}.txt"
                
                if not os.path.exists(file_path):
                    try:
                        self.logger.info(f"Downloading NAV data for {current_date.strftime('%Y-%m-%d')}...")
                        self.download_nav_file_for_date(current_date)
                        downloaded_files.append(file_path)
                        self.logger.info(f"Successfully downloaded: {file_path}")
                    except Exception as e:
                        self.logger.error(f"Failed to download for {current_date.strftime('%Y-%m-%d')}: {e}")
                else:
                    self.logger.info(f"File already exists: {file_path}")
                    downloaded_files.append(file_path)
            
            current_date -= timedelta(days=1)
        
        return downloaded_files
    
    def run_daily_job(self) -> Tuple[int, int, List[str]]:
        """
        Run the daily job to download and process the latest NAV data.
        
        Returns:
            Tuple[int, int, List[str]]: Success count, failure count, and failed dates
        """
        start_time = datetime.now()
        self.logger.info("Starting daily job")
        
        try:
            latest_db_date = self.get_latest_nav_date()
            if latest_db_date is None:
                self.logger.info("No data in database. Starting with yesterday's data.")
                latest_db_date = self.get_latest_business_day(datetime.now()).date() - timedelta(days=1)
            
            yesterday = self.get_latest_business_day(datetime.now()).date()
            
            missing_days = []
            current_date = latest_db_date + timedelta(days=1)
            while current_date <= yesterday:
                if current_date.weekday() < 5:  # Only weekdays
                    missing_days.append(current_date)
                current_date += timedelta(days=1)
            
            if not missing_days:
                self.logger.info("No missing days found. Database is up to date.")
                return 0, 0, []
            
            self.logger.info(f"Found {len(missing_days)} missing days to process")
            
            success_count = 0
            failed_count = 0
            failed_dates = []
            
            for date in missing_days:
                try:
                    self.logger.info(f"Processing data for {date.strftime('%Y-%m-%d')}")
                    
                    file_path = f"data/navall_{date.strftime('%Y-%m-%d')}.txt"
                    if not os.path.exists(file_path):
                        self.download_nav_file_for_date(datetime.combine(date, datetime.min.time()))
                    
                    df = self.parse_nav_file(file_path)
                    if df is not None and not df.empty:
                        csv_path = file_path.replace('.txt', '.csv')
                        df.to_csv(csv_path, index=False)
                        
                        self.insert_nav(df)
                        success_count += 1
                        self.logger.info(f"Successfully processed data for {date.strftime('%Y-%m-%d')}")
                    else:
                        self.logger.warning(f"No data found for {date.strftime('%Y-%m-%d')}")
                        failed_count += 1
                        failed_dates.append(date.strftime('%Y-%m-%d'))
                        
                except Exception as e:
                    failed_count += 1
                    failed_dates.append(date.strftime('%Y-%m-%d'))
                    self.logger.error(f"Error processing {date.strftime('%Y-%m-%d')}: {str(e)}")
            
            duration = datetime.now() - start_time
            self.logger.info("\nDaily Job Summary:")
            self.logger.info(f"Total days processed: {len(missing_days)}")
            self.logger.info(f"Successfully processed: {success_count}")
            self.logger.info(f"Failed to process: {failed_count}")
            if failed_dates:
                self.logger.info("Failed dates:")
                for date in failed_dates:
                    self.logger.info(f"  - {date}")
            self.logger.info(f"Total duration: {duration}")
            
            return success_count, failed_count, failed_dates
            
        except Exception as e:
            self.logger.error(f"Error in daily job: {str(e)}")
            raise
        finally:
            duration = datetime.now() - start_time
            self.logger.info(f"Daily job completed in {duration}")
    
    def run_monthly_job(self, months: int = 3) -> Tuple[int, int, List[str]]:
        """
        Run the monthly job to download and process NAV data.
        
        Args:
            months (int): Number of months to process
            
        Returns:
            Tuple[int, int, List[str]]: Success count, failure count, and failed files
        """
        start_time = datetime.now()
        self.logger.info(f"Starting monthly job for {months} months")
        
        try:
            earliest_date = self.get_earliest_nav_date()
            if earliest_date:
                end_date = earliest_date - timedelta(days=1)
                start_date = end_date - timedelta(days=months*30)
                
                self.logger.info(f"Processing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                
                downloaded_files = self.bulk_download_past_months(months, start_date, end_date)
                if not downloaded_files:
                    self.logger.warning("No files were downloaded")
                    return 0, 0, []
                
                self.logger.info(f"Downloaded {len(downloaded_files)} files")
                
                success_count = 0
                failed_count = 0
                failed_files = []
                
                for file_path in downloaded_files:
                    try:
                        self.logger.info(f"Processing file: {file_path}")
                        
                        df = self.parse_nav_file(file_path)
                        if df is None or df.empty:
                            self.logger.warning(f"No data found in file: {file_path}")
                            continue
                        
                        csv_path = file_path.replace('.txt', '.csv')
                        df.to_csv(csv_path, index=False)
                        self.logger.info(f"Saved parsed data to: {csv_path}")
                        
                        self.insert_nav(df)
                        success_count += 1
                        self.logger.info(f"Successfully processed: {file_path}")
                        
                    except Exception as e:
                        failed_count += 1
                        failed_files.append(file_path)
                        self.logger.error(f"Error processing {file_path}: {str(e)}")
                
                duration = datetime.now() - start_time
                self.logger.info("\nMonthly Job Summary:")
                self.logger.info(f"Total files processed: {len(downloaded_files)}")
                self.logger.info(f"Successfully processed: {success_count}")
                self.logger.info(f"Failed to process: {failed_count}")
                if failed_files:
                    self.logger.info("Failed files:")
                    for file in failed_files:
                        self.logger.info(f"  - {file}")
                self.logger.info(f"Total duration: {duration}")
                
                return success_count, failed_count, failed_files
                
            else:
                self.logger.info("No existing data in database. Processing default date range.")
                downloaded_files = self.bulk_download_past_months(months)
                if not downloaded_files:
                    self.logger.warning("No files were downloaded")
                    return 0, 0, []
                
                self.logger.info(f"Downloaded {len(downloaded_files)} files")
                
                success_count = 0
                failed_count = 0
                failed_files = []
                
                for file_path in downloaded_files:
                    try:
                        self.logger.info(f"Processing file: {file_path}")
                        
                        df = self.parse_nav_file(file_path)
                        if df is None or df.empty:
                            self.logger.warning(f"No data found in file: {file_path}")
                            continue
                        
                        csv_path = file_path.replace('.txt', '.csv')
                        df.to_csv(csv_path, index=False)
                        self.logger.info(f"Saved parsed data to: {csv_path}")
                        
                        self.insert_nav(df)
                        success_count += 1
                        self.logger.info(f"Successfully processed: {file_path}")
                        
                    except Exception as e:
                        failed_count += 1
                        failed_files.append(file_path)
                        self.logger.error(f"Error processing {file_path}: {str(e)}")
                
                duration = datetime.now() - start_time
                self.logger.info("\nMonthly Job Summary:")
                self.logger.info(f"Total files processed: {len(downloaded_files)}")
                self.logger.info(f"Successfully processed: {success_count}")
                self.logger.info(f"Failed to process: {failed_count}")
                if failed_files:
                    self.logger.info("Failed files:")
                    for file in failed_files:
                        self.logger.info(f"  - {file}")
                self.logger.info(f"Total duration: {duration}")
                
                return success_count, failed_count, failed_files
                
        except Exception as e:
            self.logger.error(f"Error in monthly job: {str(e)}")
            raise
        finally:
            try:
                for file_path in downloaded_files:
                    csv_path = file_path.replace('.txt', '.csv')
                    if os.path.exists(csv_path):
                        os.remove(csv_path)
                        self.logger.info(f"Cleaned up temporary file: {csv_path}")
            except Exception as e:
                self.logger.error(f"Error cleaning up temporary files: {str(e)}")
            
            self.logger.info("Monthly job completed")

def main():
    parser = argparse.ArgumentParser(description='AMFI NAV Loader - Download and process mutual fund NAV data')
    parser.add_argument('--months', type=int, default=1, help='Number of months to process (for monthly job). Default: 1')
    parser.add_argument('--yearly', type=int, default=1, help='Number of years to process (for yearly job). Default: 1')
    
    args = parser.parse_args()
    
    nav_loader = NAVLoader()
    
    if '--yearly' in sys.argv:
        nav_loader.logger.info(f"Starting yearly job for {args.yearly} years")
        nav_loader.bulk_download_past_years(args.yearly)
    elif '--months' in sys.argv:
        nav_loader.run_monthly_job(args.months)
    else:
        nav_loader.run_daily_job()

if __name__ == "__main__":
    main() 