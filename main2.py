import logging
import json
import sys
import os
from dotenv import load_dotenv
from services.sync_service import SyncService
from utils.logger import setup_logger
from utils.progress_tracker import ProgressTracker

# Load environment variables
load_dotenv()
from config.settings import OPENMRS_DB_HOST, OPENMRS_DB_USER, OPENMRS_DB_PASSWORD, OPENMRS_DB_NAME, DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD

def read_location_ids(file_path):
    """Read location IDs from a file."""
    if not os.path.isfile(file_path):
        print(f"File {file_path} does not exist. Exiting.")
        sys.exit(1)
    
    with open(file_path, 'r') as file:
        location_ids = [line.strip() for line in file if line.strip()]
    
    if not location_ids:
        print("No location IDs found in the file. Exiting.")
        sys.exit(1)
    
    return location_ids

def clear_patients_to_sync_folder():
    """Clear only the contents of the patients_to_sync directory."""
    patients_to_sync_dir = 'patients_to_sync'
    for filename in os.listdir(patients_to_sync_dir):
        file_path = os.path.join(patients_to_sync_dir, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            logging.error(f'Failed to delete {file_path}. Reason: {e}')
            sys.exit(1)

def process_location(location_id, sync_service, progress_tracker):
    """Process a single location ID."""
    print(f"Processing location ID: {location_id}")

    # Clear the contents of the patients_to_sync directory before starting
    clear_patients_to_sync_folder()

    # Initialize progress tracker
    progress_tracker = ProgressTracker('logs/progress.json')

    # Check if the location has been handled before or if it's new
    handled_encounters = progress_tracker.get_progress(location_id)
    if handled_encounters is not None:
        print(f"Location {location_id} has been handled before. Resuming the process.")
    else:
        print(f"Location {location_id} is new. Starting the process of selecting all encounters for this location.")
        handled_encounters = []
        progress_tracker.reset_progress(location_id)

    # Define encounter type IDs directly (you can replace this with a fixed list or read from a config)
    encounter_type_ids = []

    # Connect to OpenMRS and fetch encounters by location ID and encounter type IDs
    logging.info("Attempting to connect to the OpenMRS database...")
    sync_service.openmrs_connector.connect()
    logging.info("Connection to OpenMRS database successful. Fetching encounters by location ID and encounter type IDs...")
    try:
        patient_encounters = sync_service.openmrs_connector.fetch_patient_encounters_by_location(location_id, encounter_type_ids)
        if patient_encounters is None:
            logging.error(f"Failed to fetch encounters for location ID {location_id}.")
            sys.exit(1)
        logging.info(f"Fetched encounters for {len(patient_encounters)} patients from the OpenMRS database for location ID {location_id}.")
        
        # Clear patients_to_sync.json and encounters_to_process.json files
        open('patients_to_sync.json', 'w').close()
        open('encounters_to_process.json', 'w').close()
    
        # Log the fetched patient encounters to the encounters_to_process.json file and process each patient's encounters
        with open('encounters_to_process.json', 'w') as file:
            json.dump(patient_encounters, file, indent=4)
        logging.info(f"Logged encounters for {len(patient_encounters)} patients to encounters_to_process.json.")

        # Read the encounters to process from the JSON file
        with open('encounters_to_process.json', 'r') as file:
            encounters_to_process = json.load(file)

        # Loop through each patient and process their encounters
        for patient_id, encounter_ids in encounters_to_process.items():
            # Process patient and their encounters, passing the location_id
            sync_service.process_patient_and_encounters(patient_id, encounter_ids, location_id)

            # Log the processed patient ID to the progress.json file
            progress_tracker.update_progress(location_id, patient_id)
    except Exception as e:
        logging.error(f"Failed to fetch encounters by location ID: {e}")
        sys.exit(1)

    # Automatically start the synchronization process without prompting
    print("All patient files have been created in the patients_to_sync directory.")
    sync_service.dhis2_connector.process_patient_files()

def main():
    # Set up logging
    setup_logger('logs/sync.log')
    logging.info("Application started.")

    # Read location IDs from file
    location_file = 'locations.txt'
    location_ids = read_location_ids(location_file)

    # Configuration for OpenMRS and DHIS2 connectors
    openmrs_config = {
        "host": OPENMRS_DB_HOST,
        "user": OPENMRS_DB_USER,
        "password": OPENMRS_DB_PASSWORD,
        "database": OPENMRS_DB_NAME
    }
    dhis2_config = {
        "base_url": DHIS2_BASE_URL,
        "username": DHIS2_USERNAME,
        "password": DHIS2_PASSWORD
    }

    # Initialize the SyncService
    sync_service = SyncService(openmrs_config, dhis2_config, 'logs/progress.json')

    # Process each location ID from the list
    for location_id in location_ids:
        process_location(location_id, sync_service, ProgressTracker('logs/progress.json'))

if __name__ == "__main__":
    main()
