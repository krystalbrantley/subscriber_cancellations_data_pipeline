#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import json
import logging

def setup_logging():
    logging.basicConfig(filename='cleanse_db.log',
                        filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG
    )
logger = logging.getLogger(__name__)

def cleanse_student_table(df):
    """
    Cleanse the `cademycode_students` table according to the discoveries made in the writeup

    Parameters:
        df (DataFrame): `student` table from `cademycode.db`

    Returns:
        df (DataFrame): cleaned version of the input table
        missing_data (DataFrame): incomplete data that was removed for later inspection
    
    """
    df['age'] = (datetime.now() - pd.to_datetime(df['dob'])).astype('<m8[Y]')
    df['age_group'] = np.int64((df['age']/10))*10

    def parse_contact_info(contact_info):
        try:
            # Convert string to dictionary using json.loads
            contact_dict = json.loads(contact_info)
            return pd.Series(contact_dict)
        except json.JSONDecodeError:
            return pd.Series({})
    
    explode_contact = df['contact_info'].apply(parse_contact_info)
    df = pd.concat([df.drop('contact_info', axis=1).reset_index(drop=True), explode_contact], axis=1)

    split_address = df.mailing_address.str.split(',', expand=True)
    split_address.columns = ['street', 'city', 'state', 'zip_code']
    df = pd.concat([df.drop('mailing_address', axis=1), split_address], axis=1)

    df['job_id'] = df['job_id'].astype(float)
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float)
    df['age'] = df['age'].astype(int)

    missing_data = pd.DataFrame()
    missing_course_taken = df[df[['num_course_taken']].isnull().any(axis=1)]
    missing_data = pd.concat([missing_data, missing_course_taken])
    df = df.dropna(subset=['num_course_taken'])

    missing_job_id = df[df[['job_id']].isnull().any(axis=1)]
    missing_data = pd.concat([missing_data, missing_job_id])
    df = df.dropna(subset=['job_id'])

    df['current_career_path_id'] = np.where(df['current_career_path_id'].isnull(), 0, df['current_career_path_id'])
    df['time_spent_hrs'] = np.where(df['time_spent_hrs'].isnull(), 0, df['time_spent_hrs'])

    return(df, missing_data)


def cleanse_career_path(df):
    """
    Cleanse the `cademycode_courses` table according to the discoveries made in the writeup

    Parameters:
        df (DataFrame): `cademycode_courses` table from `cademycode.db`

    Returns:
        df (DataFrame): cleaned version of the input table

    """
    not_applicable = {'career_path_id': 0, 
                      'career_path_name': 'not applicable',
                      'hours_to_complete': 0}
    df.loc[len(df)] = not_applicable
    return(df)


def cleanse_student_jobs(df):
    """
    Cleanse the `cademycode_student_jobs` table according to the discoveries made in the writeup

    Parameters:
        df (DataFrame): `cademycode_student_jobs` table from `cademycode.db`

    Returns:
        df (DataFrame): cleaned version of the input table

    """
    return(df.drop_duplicates())


def test_nulls(df):
    """
    Unit test to ensure that no rows in the cleaned table are null

    Parameters:
        df (DataFrame): DataFrame of the cleansed table

    Returns:
        None
    """
    df_missing = df[df.isnull().any(axis=1)]
    count_missing = len(df_missing)

    try:
        assert count_missing == 0, "There are " + str(count_missing) + " nulls in the table."
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("No null rows found")


def test_schema(local_df, db_df):
    """
    Unit test to ensure that the column dtypes in the cleaned DataFrame match the
    column dtypes in the SQLite3 database table.

    Parameters:
        local_df (DataFrame): DataFrame of the cleansed table
        db_df (DataFrame): `cademycode_aggregated` table from `cademycode_cleansed.db`

    Returns:
        None
    """
    errors = 0
    for col in db_df:
        try:
            if local_df[col].dtypes != db_df[col].dtypes:
                errors += 1
        except NameError as ne:
            logger.exception(ne)
            raise ne
    
    if errors > 0:
        logger.exception(str(errors) + " column(s) dtypes aren't the same")
    assert errors == 0, str(errors) + " column(s) dtypes aren't the same"


def test_num_cols(local_df, db_df):
    """
    Unit test to ensure that the number of columns in the cleaned DataFrame match the
    number of columns in the SQLite3 database table.

    Parameters:
        local_df (DataFrame): DataFrame of the cleansed table
        db_df (DataFrame): `cademycode_aggregated` table from `cademycode_cleansed.db`

    Returns:
        None
    """
    try:
        assert len(local_df.columns) == len(db_df.columns)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("Number of columns are the same.")


def test_for_path_id(students, career_paths):
    """
    Unit test to ensure that join keys exist between the students and courses tables

    Parameters:
        students (DataFrame): `cademycode_student_jobs` table from `cademycode.db`
        career_paths (DataFrame): `cademycode_courses` table from `cademycode.db`

    Returns:
        None
    """
    student_table = students.current_career_path_id.unique()
    is_subset = np.isin(student_table, career_paths.career_path_id.unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing career_path_id(s): " + str(list(missing_id)) + " in 'courses' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("All career_path_ids are present.")


def test_for_job_id(students, student_jobs):
    """
    Unit test to ensure that join keys exist between the students and student_jobs tables

    Parameters:
        students (DataFrame): `cademycode_student_jobs` table from `cademycode.db`
        student_jobs (DataFrame): `cademycode_student_jobs` table from `cademycode.db`

    Returns:
        None
    """
    student_table = students.job_id.unique()
    is_subset = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subset]

    try:
        assert len(missing_id) == 0, "Missing job_id(s): " + str(list(missing_id)) + " in 'student_jobs' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        logger.info("All job_ids are present.")


def main():

    # Setup and initialize log
    setup_logging()
    logger.info("Start Log")

    # Check for current version and calculate next version for changelog
    with open('changelog.md', 'a+') as f:
        lines = f.readlines()
    if len(lines) == 0:
        next_version = 0
    else:
        #X.Y.Z
        next_version = int(lines[0].split('.')[2][0]) + 1

    # Connect to the dev database and read in the three tables
    try:
        with sqlite3.connect('cademycode.db') as conn:
            students = pd.read_sql_query("SELECT * FROM cademycode_students", conn)
            career_paths = pd.read_sql_query("SELECT * FROM cademycode_courses", conn)
            student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", conn)

        # Get the current production tables, if they exist
        try:
            with sqlite3.connect('./prod/cademycode_cleansed.db') as conn:
                clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", conn)
                missing_db = pd.read_sql_query("SELECT * FROM incomplete_data", conn)

            # Filter for students that don't exist in the cleansed database
            new_students = students[~np.isin(students.uuid.unique(), clean_db.uuid.unique())]
        except Exception as e:
            logger.exception(f"Error accessing prod database: {e}")
            new_students = students
            clean_db = []

        # Run the cleanse_student_table() function on the new students only
        clean_new_students, missing_data = cleanse_student_table(new_students)

        
        try:
            # filter for incomplete rows that don't exist in the missing data table
            new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(), missing_db.uuid.unique())]
        except Exception as e:
            logger.exception(f"Error comparing missing data: {e}")
            new_missing_data = missing_data

        # Upsert new incomplete data if there are any
        if len(new_missing_data) > 0:
            with sqlite3.connect('./dev/cademycode_cleansed.db') as sqlite_connection:
                missing_data.to_sql('incomplete_data', sqlite_connection, if_exists='append', index=False)
            
        # Proceed only if there is new student data
        if len(clean_new_students) > 0:
             # Clean the rest of the tables
            clean_career_paths = cleanse_career_path(career_paths)
            clean_student_jobs = cleanse_student_jobs(student_jobs)

            ##### UNIT TESTING BEFORE JOINING #####
            # Ensure that all required join keys are present
            test_for_job_id(clean_new_students, clean_student_jobs)
            test_for_path_id(clean_new_students, clean_career_paths)
            #######################################

            clean_new_students['job_id'] = clean_new_students['job_id'].astype(int)
            clean_new_students['current_career_path_id'] = clean_new_students['current_career_path_id'].astype(int)


            df_clean = clean_new_students.merge(
                clean_career_paths,
                left_on='current_career_path_id',
                right_on='career_path_id',
                how='left'
            )

            df_clean = df_clean.merge(
                clean_student_jobs,
                on='job_id',
                how='left'
            )

            ##### UNIT TESTING #####
            # Ensure correct schema and complete data before upserting to database
            if len(clean_db) > 0:
                test_num_cols(df_clean, clean_db)
                test_schema(df_clean, clean_db)
            test_nulls(df_clean)
            ########################

            # Upsert new cleaned data to cademycode_cleansed.db
            with sqlite3.connect('./dev/cademycode_cleansed.db') as sqlite_connection:
                df_clean.to_sql('cademycode_aggregated', sqlite_connection, if_exists='append', index=False)
                clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", sqlite_connection)
            
            # Write new cleaned data to a csv file
            clean_db.to_csv('./dev/cademycode_cleansed.csv')

            # create new automatic changelog entry
            new_lines = [
                '## 0.0.' + str(next_version) + '\n' +
                '### Added\n' +
                '- ' + str(len(df_clean)) + ' more data to the database of raw data\n' +
                '- ' + str(len(new_missing_data)) + ' new missing data to incomplete_data table\n' +
                '\n'
            ]
            w_lines = ''.join(new_lines + lines)

            # Update the changelog
            with open('./dev/changelog.md', 'w') as f:
                for line in w_lines:
                    f.write(line)
        else:
            logger.info("No new data")
    except Exception as e:
        logger.exception(f"Error in main function: {e}")

    logger.info("End Log")

if __name__ == "__main__":
    main()
