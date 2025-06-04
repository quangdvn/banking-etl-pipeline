# Import required modules
import os
import sys

from src.orchestration.main import BankingETLPipeline

# Add project directory to Python path
project_path = "/dbfs/FileStore/banking-etl-pipeline"
sys.path.append(project_path)

# Import the main pipeline class

# Set configuration path
config_path = os.path.join(project_path, "config/config.json")

# Initialize and run the pipeline
pipeline = BankingETLPipeline(config_path)
pipeline.run_pipeline()
