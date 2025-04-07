import os
import logging
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from db_operations import save_options_data, initialize_database

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def migrate_blob_to_sql():
    """
    Migrate options data from Azure Blob Storage to Azure SQL Database.
    """
    try:
        # Initialize database
        initialize_database()
        
        # Get blob service client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_BLOB_CONNECTION_STRING")
        )
        
        # Get container client
        container_name = "options-data"
        container_client = blob_service_client.get_container_client(container_name)
        
        if not container_client.exists():
            logging.error(f"Container {container_name} does not exist")
            return
        
        # List all blobs in the container
        blobs = container_client.list_blobs()
        
        # Process each blob
        for blob in blobs:
            try:
                # Get blob name and extract ticker
                blob_name = blob.name
                ticker = blob_name.split('_')[0]
                
                logging.info(f"Processing blob: {blob_name}")
                
                # Download blob content
                blob_client = container_client.get_blob_client(blob_name)
                blob_content = blob_client.download_blob().readall()
                
                # Convert to DataFrame
                df = pd.read_csv(StringIO(blob_content.decode('utf-8')))
                
                # Save to database
                save_options_data(df, ticker)
                
                logging.info(f"Successfully migrated {blob_name} to database")
                
            except Exception as e:
                logging.error(f"Error processing blob {blob_name}: {str(e)}")
                continue
        
        logging.info("Migration completed")
        
    except Exception as e:
        logging.error(f"Error during migration: {str(e)}")

if __name__ == "__main__":
    migrate_blob_to_sql() 