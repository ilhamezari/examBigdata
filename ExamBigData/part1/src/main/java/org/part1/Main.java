package org.part1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("VOLSApp")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> volsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_AEROPORT")
                .option("dbtable", "VOLS")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> reservationsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_AEROPORT")
                .option("dbtable", "RESERVATIONS")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> passagersDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_AEROPORT")
                .option("dbtable", "PASSANGERS")
                .option("user", "root")
                .option("password", "")
                .load();

        volsDF.createOrReplaceTempView("VOLS");
        reservationsDF.createOrReplaceTempView("RESERVATIONS");
        passagersDF.createOrReplaceTempView("PASSAGERS");

        // Afficher pour charque vol, le nombre de passagers
        Dataset<Row> result = spark.sql(

                "SELECT v.ID, v.DATE_DEPART, COUNT(r.ID_PASSAGER) AS NOMBRE " +
                        "FROM VOLS v LEFT JOIN RESERVATIONS r ON v.ID = r.ID_VOL " +
                        "GROUP BY v.ID, v.DATE_DEPART"


        );

        // Afficher la liste des vols en cours
        Dataset<Row> currentVols = spark.sql(

                        "SELECT * FROM VOLS WHERE DATE_DEPART = current_date();"
                );


        result.show((int) result.count());
        currentVols.show();
        spark.stop();
    }
}
