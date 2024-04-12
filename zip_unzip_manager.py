import io
import sys
import zipfile
import os
import logging
from typing import Tuple, Union
from pyspark.sql import SparkSession, DataFrame, Row

class ZipUnzipManager:
    def __init__(self, action: Union[str, None], input_file_path: str, output_dir_path: str,
                 max_part_size_gb: int, min_input_file_size_gb: int):
        """
        Initializes the ZipUnzipManager.

        Args:
            action (Union[str, None]): Action to perform ('zip' or 'unzip').
            input_file_path (str): Input file path.
            output_dir_path (str): Output directory path.
            max_part_size_gb (int): Maximum part size in gigabytes (GB).
            min_input_file_size_gb (int): Minimum input file size in gigabytes (GB).
        """
        self.action = action
        self.input_file_path = input_file_path
        self.output_dir_path = output_dir_path
        self.max_part_size_bytes = max_part_size_gb * 1024 * 1024 * 1024
        self.min_input_file_size_bytes = min_input_file_size_gb * 1024 * 1024 * 1024

    def create_unzip(self, x: Tuple[str, bytes]) -> bytes:
        """
        Unzips the input binary data.

        Args:
            x (Tuple[str, bytes]): A tuple containing the filename and binary data.

        Returns:
            bytes: Unzipped file data.
        """
        in_memory_data = io.BytesIO(x[1])
        file_object = zipfile.ZipFile(in_memory_data, 'r')
        file_data = file_object.read('example.txt')

        return file_data

    def create_zip_parts(self, x: Tuple[str, bytes]) -> None:
        """
        Creates multi-part zip files from the input binary data.

        Args:
            x (Tuple[str, bytes]): A tuple containing the filename and binary data.
        """
        in_memory_data = io.BytesIO(x[1])
        file_object = zipfile.ZipFile(in_memory_data, 'r')
        file_data = file_object.read('example.txt')

        # Calculate the number of parts needed
        num_parts = (len(file_data) + self.max_part_size_bytes - 1) // self.max_part_size_bytes

        # Create zip files for each part
        for i in range(num_parts):
            part_data = file_data[i * self.max_part_size_bytes: (i + 1) * self.max_part_size_bytes]
            part_filename = os.path.join(self.output_dir_path, f'part_{i + 1}.zip')

            with zipfile.ZipFile(part_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr('example.txt', part_data)

    def process(self) -> None:
        """
        Processes the specified action (zip or unzip).
        """
        spark = SparkSession.builder.appName('zip_unzip').getOrCreate()
        sc = spark.sparkContext

        if self.action == 'unzip':
            try:
                zips = sc.binaryFiles(self.input_file_path).repartition(5)
                row = Row('val')
                map_data = zips.map(lambda x: row(self.create_unzip(x[1])))
                df = spark.createDataFrame(map_data)

                df.write.mode('overwrite').format('text').option('compression', 'none').option(
                    'encoding', 'UTF-8').save(self.output_dir_path)

                logging.info(f"Successfully unzipped and saved to: {self.output_dir_path}")
            except Exception as excep:
                logging.error(excep)
                exit(-1)
        elif self.action == 'zip':
            try:
                # Load the input file using sc.binaryFiles
                zips = sc.binaryFiles(self.input_file_path)
                row = Row('val')
                map_data = zips.map(lambda x: row(self.create_zip_parts(x)))
                df = spark.createDataFrame(map_data)

                df.write.mode('overwrite').format('text').option('compression', 'none').option(
                    'encoding', 'UTF-8').save(self.output_dir_path)

                input_file_size_bytes = os.path.getsize(self.input_file_path[7:])
                if input_file_size_bytes > self.min_input_file_size_bytes:
                    logging.info(f"Successfully created multi-part zip files in: {self.output_dir_path}")
                else:
                    logging.info(f"Input file size is less than {self.min_input_file_size_bytes / (1024 * 1024 * 1024):.2f} GB. No multi-part zip files created.")
            except Exception as excep:
                logging.error(excep)
                exit(-1)
        else:
            logging.error("Invalid action. Please use 'unzip' or 'zip' as")
