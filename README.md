# Subscriber Cancellations Data Pipeline

## Project Description
- **Overview**:
  - A mock database of long-term cancelled subscribers for a fictional subscription company is regularly updated from multiple sources, and needs to be routinely cleaned and transformed into usable shape with as little human intervention as possible.
- **Purpose**:
  - To build a data engineering pipeline to regularly transform a messy database into a clean source of truth for an analytics team.
- **Goal**:
  A semi-automated pipeline:
    - Performs unit tests to confirm data validity
    - Writes human-readable errors to an error log
    - Automatically checks and updates changelogs
    - Updates a production database with new clean data

## Folder Structure

- **dev/**: Development directory
  - **changelog.md**: Changelog file to track updates to the database
  - **cademycode.db**: Original database containing raw data from 3 tables (`cademycode_students`, `cademycode_courses`, `cademycode_student_jobs`)
  - **cademycode_cleansed.db**: Cleansed database (created during the update process)
      - contains 2 tables: `cademycode_aggregated` and `missing_data`
  
- **prod/**: Production directory
  - **changelog.md**: `my_script.sh` will copy from /dev when updates are approved
  - **cademycode_cleansed.db**: `my_script.sh` will copy from /dev when updates are approved
  - **cademycode_cleansed.csv**: Aggregated data in CSV format for production use

- **writeup/**:
  - **writeup.md**: High-level overview of the project
  - **subscriber_cancellations.ipynb**: Jupyter Notebook containing the discovery phase of this project: loading, inspecting, transforming.

## Python Script

- `clean_data.py`: Python script for updating and cleansing the database.

## Bash Script

- `my_script.sh`: Bash script to handle running the Python script and copying updated files from the development directory to the production directory.


## Instructions

1. Make sure you have the required dependencies installed (Python, pandas, numpy).

2. Navigate to the project's root directory.

3. Run the `my_script.sh` and follow the prompts to update the database.

4. If prompted, `my_script.sh` will run `dev/clean_data.py`, which runs unit tests and data cleaning functions on `dev/cademycode.db`

5. If `clean_data.py` runs into any errors during unit testing, it will raise an exception, log the issue, and terminate

6. Otherwise, `clean_data.py` will update the clean database and CSV with any new records

7. After a successful update, the number of new records and other update data will be written to `dev/changelog.md`

8. `my_script.sh` will check the changelog to see if there are updates

9. If so, `my_script.sh` will request permission to overwrite the production database

10. If the user grants permission, `my_script.sh` will copy the updated database to prod
