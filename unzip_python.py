import io
import sys
import zipfile
from pyspark.sql import SparkSession, Row


def unzip(x):
    in_memory_data = io.BytesIO(x[1])
    file_object = zipfile.ZipFile(in_memory_data, 'r')
    file_data = file_object('example.txt')

    return file_data


if __name__ == '__main__':
    spark = SparkSession.builder.appName('unzip').getOrCreate()
    sc = spark.sparkContext

    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    if sys.argv[1] == 'unzip':
        try:
            zips = sc.binaryFiles('file://input/example.txt.zip').repartition(5)
            row = Row('val')
            map_data = zips.map(unzip).map(row)
            df = spark.createDataFrame(map_data)

            df.write.mode('overwrite').format('text').option('compression', 'none').option(
                'encoding', 'UTF-8').save('file://output/')

        except Exception as excep:
            print(excep)
            exit(-1)
