# AMFI NAV Loader - User Guide

## License
This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright 2025

This guide explains how to use the script to download and process mutual fund NAV (Net Asset Value) data from the AMFI India portal.

## Prerequisites

- Python 3.8 or higher
- MySQL database
- Required Python packages (install using `pip install -r requirements.txt`)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/ukkit/nav_loader
cd nav_loader
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up environment variables (optional):
```bash
# Database configuration (default values shown)
export MYSQL_HOST=mysqldb
export MYSQL_USER=kirk
export MYSQL_PASSWORD=james
export MYSQL_DATABASE=enterprise
export MYSQL_PORT=3306
```

## Database Setup

Create the database and table using the provided schema.sql file:

```bash
# Create the database
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS enterprise;"

# Import the schema
mysql -u root -p enterprise < schema.sql
```

The schema creates a partitioned table with the following structure:
```sql
CREATE TABLE nav_data (
    id INT AUTO_INCREMENT,
    scheme_type VARCHAR(100),
    scheme_category VARCHAR(100),
    scheme_sub_category VARCHAR(100),
    scheme_code VARCHAR(20),
    isin_growth VARCHAR(30),
    isin_reinv VARCHAR(30),
    scheme_name TEXT,
    nav DECIMAL(10, 4),
    nav_date DATE NOT NULL,
    fund_structure VARCHAR(100),
    PRIMARY KEY (id, nav_date),
    INDEX(nav_date),
    INDEX(scheme_code),
    UNIQUE KEY unique_scheme_date (scheme_code, nav_date)
)
PARTITION BY RANGE (YEAR(nav_date)) (
    -- Partitions from 1990 to 2024
    PARTITION p1990 VALUES LESS THAN (1991),
    -- ... (intermediate partitions)
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);
```

Key features of the database design:
- Partitioned by year for better performance
- Indexed on nav_date and scheme_code
- Unique constraint on scheme_code and nav_date combination
- Appropriate data types for each column
- Text type for scheme_name to handle long names

## Usage

The NAV Loader provides three main modes of operation:

### 1. Daily Job

Runs the daily job to download and process the latest NAV data. This mode:
- Checks for missing days in the database
- Downloads data for missing business days
- Processes and inserts the data into the database
- Skips weekends and Indian public holidays

```bash
python app/nav_loader.py
```

### 2. Monthly Job

Downloads and processes NAV data for a specified number of past months. This mode:
- Starts from the earliest date in the database
- Goes back for the specified number of months
- Skips weekends and Indian public holidays
- Handles rate limiting and retries

```bash
python app/nav_loader.py --months 3
```

### 3. Yearly Job

Downloads and processes NAV data for a specified number of past years. This mode:
- Starts from the earliest date in the database
- Goes back for the specified number of years
- Skips weekends and Indian public holidays
- Handles rate limiting and retries

```bash
python app/nav_loader.py --yearly 5
```

## Configuration

The system uses the following configuration parameters:

### Rate Limiting
- Minimum delay between requests: 2 seconds
- Maximum delay between requests: 5 seconds
- Maximum retries for failed requests: 3
- Delay between retries: 10 seconds

### Database Configuration
- Host: mysqldb (configurable via MYSQL_HOST)
- User: bob (configurable via MYSQL_USER)
- Password: marley (configurable via MYSQL_PASSWORD)
- Database: dont_worry (configurable via MYSQL_DATABASE)
- Port: 3306 (configurable via MYSQL_PORT)

## Output

The system creates the following directories and files:

- `data/`: Contains downloaded NAV files in TXT format
- `logs/`: Contains log files with detailed operation information
- CSV files: Temporary CSV files are created during processing and automatically cleaned up

## Logging

The system logs all operations to both console and file. Log files are stored in the `logs/` directory with the following information:
- Operation start and end times
- Success and failure counts
- Detailed error messages including:
  - Download failures with response content
  - Database errors with error codes
  - Invalid data rows with examples
  - Affected schemes
- Processing statistics
- Summary of failed operations

## Error Handling

The system includes comprehensive error handling:
- Retries failed downloads up to 3 times with exponential backoff
- Skips invalid data rows and logs details
- Handles database errors with transaction rollback
- Continues processing even if some files fail
- Provides detailed error messages for debugging

## Business Day Logic

The system considers a day as a business day if:
- It's not a weekend (Saturday or Sunday)
- It's not an Indian public holiday

Public holidays are determined using the `holidays` package with the Indian holiday calendar.

## Data Validation

The system performs the following validations:
- Checks for required columns
- Validates NAV values (must be numeric)
- Validates dates (must be in correct format)
- Cleans scheme codes and names
- Removes empty rows
- Handles duplicate entries

## Best Practices

1. Run the daily job regularly to keep the database up to date
2. Use the monthly/yearly jobs to fill historical gaps
3. Monitor the log files for any issues
4. Ensure sufficient disk space for downloaded files
5. Keep the database backed up regularly
6. Monitor database partition usage
7. Regularly check for failed operations in logs

## Troubleshooting

Common issues and solutions:

1. **Database Connection Errors**
   - Verify database credentials
   - Check if the database server is running
   - Ensure the database table exists
   - Check partition status

2. **Download Failures**
   - Check internet connection
   - Verify AMFI portal accessibility
   - Monitor rate limiting settings
   - Check response content in logs

3. **Data Processing Errors**
   - Check log files for specific error messages
   - Verify file permissions
   - Ensure sufficient disk space
   - Check for invalid data patterns

4. **Partition Issues**
   - Monitor partition usage
   - Add new partitions for future years
   - Check partition maintenance

## Support

For issues or questions, please:
1. Check the log files for detailed error information
2. Review the documentation
3. Contact the development team if needed