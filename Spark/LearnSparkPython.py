from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re

sparkConf = SparkConf().setMaster("local").setAppName("Python Spark Playground")
sparkContext = SparkContext(conf=sparkConf)
sparkSession = SparkSession(sparkContext)

# membuar RDD
story = "Pada jaman dahulu Zetra belum mandi, sampai sekarang belum mandi. Terus dia merasa gatal, dan akhirnya " \
        "mandi. Selesai. "
rddStory = sparkContext.parallelize([story])
lengthStory = rddStory.flatMap(lambda sentence: sentence.split(" ")) \
    .filter(lambda word: word.startswith('D') or word.startswith('d')) \
    .count()
print(lengthStory)

# print data story tapi bersih
rddStory.map(lambda sentence: re.sub(r"[,.\\-_\\?]", "", sentence.lower())) \
    .foreach(lambda sentence: print(sentence))

# ambil file nanti bentuknya semacam list
rddFile = sparkContext.textFile("/Users/rya.meyvriska/Downloads/mock_data.csv")
for v in rddFile.take(3):
    print(v)

# membaca file ke dataframe
dataFrameFile = sparkSession.read.option("header", "true").csv("/Users/rya.meyvriska/Downloads/mock_data.csv")
dataFrameFile.show()
dataFrameFile.describe().show()


# menambah column company
@udf()
def mapping_function_company(email):
    sub_by_at = email[email.find("@"):]
    return sub_by_at[1:sub_by_at.find('.')]


dataFrameFile = dataFrameFile.withColumn("company", mapping_function_company("email"))


# menambah kolom fullname
@udf()
def mapping_function_fullname(first_name, last_name):
    return first_name + " " + last_name


dataFrameFile = dataFrameFile.withColumn("full_name", mapping_function_fullname("first_name", "last_name"))


# menumerikan gender
@udf("int")
def mapping_function_gender(gender):
    if gender == "Female":
        return 0
    else:
        return 1


dataFrameFile = dataFrameFile.withColumn("gender", mapping_function_gender("gender"))
dataFrameFile.show()

# membuat dataframe baru
schemaGender = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

dataGender = sparkSession.createDataFrame([(0, "Female"), (1, "Male")], schemaGender)
dataGender.show()

# join dengan table lama
print(dataFrameFile.columns)
dataFrameFile.join(dataGender, dataFrameFile.gender == dataGender.id) \
    .select(dataFrameFile.id, dataFrameFile.full_name, dataGender.name.alias("gender")) \
    .show()

# save csv
dataFrameFile.write.option("header", "true").csv("/Users/rya.meyvriska/Downloads/result_pyspark.csv")
