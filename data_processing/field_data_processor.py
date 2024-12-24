import pandas as pd
from data_processing.data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging


class FieldDataProcessor:
    """
    A class for processing field data.

    Parameters:
    - config_params (dict): Configuration parameters for data processing.
    - logging_level (str, optional): Logging level for the class (default is "INFO").

    Attributes:
    - db_path (str): Database path.
    - sql_query (str): SQL query for data retrieval.
    - columns_to_rename (dict): Columns to be renamed in the DataFrame.
    - values_to_rename (dict): Values to be renamed in a specific column.
    - weather_map_data (str): CSV file path for weather station mapping.
    - logger (logging.Logger): Logger object for logging messages.
    - df (pd.DataFrame): DataFrame to store the processed data.
    - engine: Database engine for data retrieval.

    Methods:
    - initialize_logging(logging_level): Set up logging for the instance.
    - ingest_sql_data(): Create the engine and read data from SQL.
    - rename_columns(): Rename specified columns in the DataFrame.
    - apply_corrections(column_name='Crop_type', abs_column='Elevation'): Apply corrections to specified columns.
    - weather_station_mapping(): Read data from the weather station mapping CSV.
    - process(): Execute all methods in the correct order for data processing.
    """
    def __init__(self, config_params, logging_level="INFO"):  # Make sure to add this line, passing in config_params to the class 
        """
        Initialize the FieldDataProcessor instance.

        Parameters:
        - config_params (dict): Configuration parameters for data processing.
        - logging_level (str, optional): Logging level for the class (default is "INFO").
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']
        
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
    

    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Set up logging for this instance of FieldDataProcessor.

        Parameters:
        - logging_level (str): Logging level for the class.
        """
        logger_name = __name__ + ".FieldDataProcessor"
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

        # Use self.logger.info(), self.logger.debug(), etc.
            

    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        Create the engine and read data from SQL.

        Returns:
        - pd.DataFrame: Processed DataFrame.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df


    def rename_columns(self):
        """
        Rename specified columns in the DataFrame.

        Returns:
        - pd.DataFrame: Processed DataFrame with renamed columns.
        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]       
        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        self.logger.info(f"Swapped columns: {column1} with {column2}")
        return self.df
        
            
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Apply corrections to specified columns.

        Parameters:
        - column_name (str, optional): Name of the column to apply corrections (default is 'Crop_type').
        - abs_column (str, optional): Name of the column for absolute correction (default is 'Elevation').

        Returns:
        - pd.DataFrame: Processed DataFrame with applied corrections.
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))
        return self.df

    
    def weather_station_mapping(self):
        """
        Read data from the weather station mapping CSV.

        Returns:
        - pd.DataFrame: DataFrame with weather station mapping data.
        """
        return read_from_web_CSV(self.weather_map_data)
    
    
    def process(self):
        """
        Execute all methods in the correct order for data processing.

        Returns:
        - pd.DataFrame: Processed DataFrame after all data processing steps.
        """
        self.df = self.ingest_sql_data()
        self.df = self.rename_columns()
        self.df = self.apply_corrections()
        weather_map_df = self.weather_station_mapping() 
        self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')
        self.df = self.df.drop(columns="Unnamed: 0")