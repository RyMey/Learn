import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

public class LearnSparkJava {
    public static void main(String[] args) {
        // konfigrasi spark context (aplikasi spark)
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("learn-spark");

        // membuat spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        // khusus untuk dataset
        SparkSession sparkSession = new SparkSession(sparkContext.sc());

        String story = "Pada jaman dahulu Zetra belum mandi, sampai sekarang belum mandi. Terus dia merasa gatal, " +
                "dan akhirnya mandi. Selesai.";
        // bikin semacam list di spark dari story yg udah dibikin, karena isinya 1 jadinya singletonList
        JavaRDD<String> rddStory = sparkContext.parallelize(Collections.singletonList(story));
        // ambil kata yg depannya d
        long wordStartWithDCount = rddStory.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(word -> word.startsWith("d") || word.startsWith("D"))
                .count();
        System.out.println(wordStartWithDCount);

        // print data story tapi bersih
        rddStory.map(sentence -> sentence.toLowerCase().replaceAll("[,.\\-_\\?]", ""))
                .foreach(sentence -> System.out.println(sentence));

        // ambil file nanti bentuknya semacam list
        JavaRDD<String> rddFile = sparkContext.textFile("/Users/rya.meyvriska/Downloads/mock_data.csv");
        rddFile.take(3).forEach(element -> System.out.println(element));

        // membaca file ke dataset
        Dataset<Row> datasetFile = sparkSession.read()
                .option("header", "true")
                .csv("/Users/rya.meyvriska/Downloads/mock_data.csv");
        datasetFile.show();
        datasetFile.describe().show();

        // menambah column company
        UserDefinedFunction mappingFunctionCompany = udf(
                (String email) -> {
                    String subByAt = email.substring(email.indexOf('@'));
                    return subByAt.substring(1, subByAt.indexOf('.'));
                },
                DataTypes.StringType
        );

        datasetFile = datasetFile.withColumn("company", mappingFunctionCompany.apply(
                datasetFile.col("email")
        ));

        // menambahkan kolom fullname
        UserDefinedFunction mappingFunctionFullName = udf(
                (String firstName, String lastName) -> firstName + ' ' + lastName, DataTypes.StringType
        );
        datasetFile = datasetFile.withColumn("full_name", mappingFunctionFullName.apply(
                datasetFile.col("first_name"), datasetFile.col("last_name")
        ));

        datasetFile.groupBy("gender").count().show();

        // menumerikan gender
        UserDefinedFunction mappingGender = udf(
                (String gender) -> {
                    if (gender.equals("Female"))
                        return 0;
                    else
                        return 1;
                }, DataTypes.IntegerType
        );

        datasetFile = datasetFile.withColumn("gender", mappingGender.apply(datasetFile.col("gender")));
        datasetFile.show();
        datasetFile.groupBy("company", "gender").count().sort(desc("count")).show();
        datasetFile.filter("company = 'google' and gender = 0").show();
        datasetFile = datasetFile.withColumn("status", lit(false));
        datasetFile = datasetFile.drop("status");
        datasetFile.show();

        // membuat dataframe baru
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        });

        Dataset<Row> dataGender = sparkSession.createDataFrame(Arrays.asList(
                RowFactory.create(0, "Female"),
                RowFactory.create(1, "Male")
        ), schema);
        dataGender.show();

        // join dengan table lama
        datasetFile.join(dataGender, datasetFile.col("gender").equalTo(dataGender.col("id")))
                .select(
                        datasetFile.col("id"),
                        datasetFile.col("full_name"),
                        dataGender.col("name").alias("gender")
                )
                .show();

        // save csv
        datasetFile.write().option("header", "true").csv("/Users/rya.meyvriska/Downloads/result");
    }
}
