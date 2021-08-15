package net.jgp.books.spark.ch11.lab100_simple_select;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple SQL select on ingested data
 *
 * @author jgp
 */
public class SimpleSelectApp {

    //The WHERE clause appears mainly to filter out null values (and limiting the number of records to ease memory use)
    public static final String SELECT_THE_5_LOCATIONS_WITH_LOWEST_POPULATION =
            "SELECT * FROM geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5";
    public static final String SELECT_THE_5_LOCATIONS_WITH_LOWEST_POPULATION_V02 =
            "SELECT * FROM geodata  ORDER BY yr1980 LIMIT 5";

    public static final String SELECT_THE_5_LOCATIONS_WITH_HIGHEST_POPULATION =
            "SELECT * FROM geodata  ORDER BY yr1980 DESC LIMIT 5";

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        SimpleSelectApp app = new SimpleSelectApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "geo",
                        DataTypes.StringType,
                        true),
                DataTypes.createStructField(
                        "yr1980",
                        DataTypes.DoubleType,
                        false)});

        // Reads a CSV file with header, called books.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");

        //without filtering its 232
        long number_of_records_unfiltered = df.count();
        System.out.println("number_of_records=" + number_of_records_unfiltered);

        //create a filtered dataframe to through out null values
        Dataset<Row> filtered_df = df.filter(df.col("yr1980").isNotNull());

        // Create a view 'geodata' in memory that can be queried based on the filtered dataframe
        filtered_df.createOrReplaceTempView("geodata");

        filtered_df.printSchema();
        filtered_df.show(50);

        //filtering out null values, its 199
        long number_of_records_filtered = filtered_df.count();
        System.out.println("number_of_records_filtered=" + number_of_records_filtered);

        Dataset<Row> smallCountries =
                spark.sql(
                        SELECT_THE_5_LOCATIONS_WITH_LOWEST_POPULATION_V02);

        // Shows at most 10 rows from the dataframe (which is limited to 5
        // anyway)
        smallCountries.show(10, false);

        Dataset<Row> biggestCountries =
                spark.sql(
                        SELECT_THE_5_LOCATIONS_WITH_HIGHEST_POPULATION);

        // Shows at most 10 rows from the dataframe (which is limited to 5
        // anyway)
        biggestCountries.show(10, false);
    }
}
