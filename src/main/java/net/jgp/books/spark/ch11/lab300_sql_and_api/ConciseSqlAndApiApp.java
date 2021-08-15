package net.jgp.books.spark.ch11.lab300_sql_and_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.immutable.Seq;

import java.util.List;
import static java.util.stream.Collectors.toList;
import java.util.stream.IntStream;


/**
 * Simple SQL select on ingested data after preparing the data with the
 * dataframe API.
 * Modifies the dataframe only for relevant columns casting their type to double and dropping the others rather than
 * imposing a schema defining all the columns we later will drop anyway.
 * Just to explore some alternative approaches.
 * @author jgp
 */
public class ConciseSqlAndApiApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        ConciseSqlAndApiApp app = new ConciseSqlAndApiApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Create a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Simple SQL")
                .master("local")
                .getOrCreate();


        // Reads a CSV file with header (as specified in the schema), called
        // populationbycountry19802010millions.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", "true") // instead of extensive schema definitions
                .load("data/populationbycountry19802010millions.csv");

        // first inferred schema
        df.printSchema();


        // creating an scala immutable Seq<String> of all the names of the column we want to get rid off
        // With IntStream you can define a nice range, but you need mapToObj to map the integers to strings
        List<String> columnList = IntStream.rangeClosed(1980, 2010).mapToObj(Integer::toString).collect(toList());

        // a bit obscure conversion of a java.util.List<String> to a scala.collection.immutable.Seq
        // as this is the acceptable type for the drop() method argument
        Seq<String> columnsToDiscard = scala.collection.JavaConversions.asScalaBuffer(columnList).toList();

        df = df.withColumnRenamed(df.columns()[0], "geo") // renaming the first column to 'geo'

                //add a column yr1980 and yr2010 based on 1980 and 2010 resp. and cast them to double and make them not nullable
                .withColumn("yr1980", df.col("1980").cast(DataTypes.DoubleType))
                .withColumn("yr2010", df.col("2010").cast(DataTypes.DoubleType))
                .drop(columnsToDiscard) // drop all the columns with the simple year names
                .na().drop(); //another way to ensure no nullpointers dropping any record containing any null column


        // schema after modifications to the dataframe
        df.printSchema();

        // Creates a new column with the evolution of the population between
        // 1980
        // and 2010
        //without 'yr' prefix in the column names you end up subtracting the numbers 2010 - 1980 = 30
        df = df.withColumn(
                "evolution",
                functions.expr("round((yr2010 - yr1980) * 1000000)"));
        df.createOrReplaceTempView("geodata");

        Dataset<Row> negativeEvolutionDf =
                spark.sql(
                        "SELECT * FROM geodata "
                                //+ "WHERE geo IS NOT NULL AND evolution<=0 "
                                + "ORDER BY evolution "
                                + "LIMIT 25");

        // Shows at most 15 rows from the dataframe
        negativeEvolutionDf.show(15, false);

        Dataset<Row> moreThanAMillionDf =
                spark.sql(
                        "SELECT * FROM geodata "
                                //+ "WHERE geo IS NOT NULL AND evolution>999999 " //where clause is rather presumptuous
                                + "ORDER BY evolution DESC "
                                + "LIMIT 25");
        moreThanAMillionDf.show(15, false);
    }
}
