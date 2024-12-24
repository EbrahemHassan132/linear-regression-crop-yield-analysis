import re
import numpy as np
import pandas as pd
import logging
from data_processing.data_ingestion import read_from_web_CSV


class WeatherDataProcessor:
    """
    A class for processing weather station data.

    Parameters:
    - config_params (dict): Configuration parameters for data processing.
    - logging_level (str, optional): Logging level for the class (default is "INFO").

    Attributes:
    - weather_station_data (str): CSV file path for weather station data.
    - patterns (dict): Regular expression patterns for extracting measurements from messages.
    - weather_df (pd.DataFrame): DataFrame to store weather station data.
    - logger (logging.Logger): Logger object for logging messages.

    Methods:
    - initialize_logging(logging_level): Set up logging for the instance.
    - weather_station_mapping(): Load weather station data from the web.
    - extract_measurement(message): Extract measurements from a given message using regex patterns.
    - process_messages(): Process messages in the DataFrame to extract measurements.
    - calculate_means(): Calculate mean values for each weather station and measurement.
    - process(): Execute all methods in the correct order for data processing.
    """
    def __init__(self, config_params, logging_level="INFO"): # Now we're passing in the confi_params dictionary already
        """
        Initialize the WeatherDataProcessor instance.

        Parameters:
        - config_params (dict): Configuration parameters for data processing.
        - logging_level (str, optional): Logging level for the class (default is "INFO").
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Set up logging for this instance of WeatherDataProcessor.

        Parameters:
        - logging_level (str): Logging level for the class.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Load weather station data from the web.

        Returns:
        - pd.DataFrame: Weather station data DataFrame.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
        # Here, you can apply any initial transformations to self.weather_df if necessary.

    
    def extract_measurement(self, message):
        """
        Extract measurements from a given message using regex patterns.

        Parameters:
        - message (str): The message containing weather measurements.

        Returns:
        - tuple or None: A tuple containing the measurement key and value, or None if no match is found.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Process messages in the DataFrame to extract measurements.

        Returns:
        - pd.DataFrame: Processed DataFrame with extracted measurements.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculate mean values for each weather station and measurement.

        Returns:
        - pd.DataFrame or None: DataFrame with mean values or None if weather_df is not initialized.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
        Execute all methods in the correct order for data processing.

        Returns:
        - None
        """
        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        self.logger.info("Data processing completed.")

