import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameNaFunctions;
import java.io.File;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.*;

public class DistancePerPlayer {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("JS").config("sth","Sth").getOrCreate();
    // String folder_path = "/cw/bdap/assignment2/nba_movement_data/moments/0021500548.csv";
    String folder_path = "/data/nba_movement_data/moments";
    Dataset<Row> df = spark.read().option("header","true").csv(folder_path);

    //filter out ball data 
    df = df.filter(functions.col("player_id").gt(-1)); 

    //drop columns of no interest for this question (radius, event_id) for simplification. We assume that each player has unique id so team_id is of no need
    df = df.drop("radius","event_id","team_id");

    //cast columns to respective type and convert feet to meters
    df = df.withColumn("x_loc",functions.col("x_loc").cast("float").multiply(0.3048));
    df = df.withColumn("y_loc",functions.col("y_loc").cast("float").multiply(0.3048));
    df = df.withColumn("player_id",functions.col("player_id").cast("int"));
    df = df.withColumn("game_id",functions.col("game_id").cast("int"));    
    df = df.withColumn("game_clock",functions.col("game_clock").cast("float"));
    df = df.withColumn("shot_clock",functions.col("shot_clock").cast("float"));
    df = df.withColumn("quarter",functions.col("quarter").cast("int"));

    df.show();

    //create column for x_loc, y_loc containing difference of current and next distance. If null (last distance in each quarter), then make it zero. Square differences 
    Column newcol_x = functions.col("x_loc").minus(functions.lag(functions.col("x_loc"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())));
    newcol_x = functions.when(newcol_x.isNull(),0.0).otherwise(newcol_x.multiply(newcol_x));
    Column newcol_y = functions.col("y_loc").minus(functions.lag(functions.col("y_loc"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())));
    newcol_y = functions.when(newcol_y.isNull(),0.0).otherwise(newcol_y.multiply(newcol_y));

    //add distance (sqrt of sum of x and y dimensions' squared differences) if game_clock progresses, meaning the game is running
    Column distance = functions.when(functions.col("game_clock").equalTo(functions.lag(functions.col("game_clock"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc()))), 0.0).otherwise(functions.sqrt(newcol_x.plus(newcol_y)));
    df = df.withColumn("distance", distance).groupBy("game_id","player_id").agg(functions.sum("distance").alias("distance"));

    df.show(100,false);

    //load dataset that contains minutes played for normalization
    // String folder_path_minutes = "/cw/bdap/assignment2/nba_movement_data/minutes_played.csv";
    String folder_path_minutes = "/data/nba_movement_data/minutes_played.csv";
    Dataset<Row> df_moments = spark.read().option("header","true").csv(folder_path_minutes);

    //add minutes column to minutes played dataset
    df_moments = df_moments.withColumn("MIN",functions.col("SEC").cast("float").multiply((float)1/60));

    //rename columns to avoid ambiguity with df columns after merging
    df = df.withColumnRenamed("player_id","player").withColumnRenamed("game_id","game");

    //join dataframes based on game_id and player_id
    Dataset<Row> joined = df.join(df_moments,df.col("player").equalTo(df_moments.col("PLAYER_ID")).and(df.col("game").equalTo(df_moments.col("GAME_ID"))),"inner");
    
    //normalize distance run per quarter
    joined = joined.withColumn("distance_per_quarter",functions.col("distance").multiply(12).divide(functions.col("MIN")));
    joined.show(100, false);

    //average over season
    Dataset<Row> av = joined.groupBy("player").agg(functions.avg("distance_per_quarter").cast("int").alias("int_distance"));
    av.show(100,false);

    //write results to csv file
    av.select(functions.col("player").alias("player_ID"),functions.col("int_distance").alias("distance")).coalesce(1).write().mode("overwrite").option("delimiter"," ").option("header","true").csv("./output_dist");

    //access dfs directory and copy it locally
    Configuration conf = new Configuration();
    Path inputPath = new Path("/user/r1028141/output_dist");
    Path outputPath = new Path(".");
    FileSystem fs = null;
    try {
      fs = FileSystem.get(conf);
      if(fs.exists(inputPath)) {
          fs.copyToLocalFile(inputPath, outputPath);
      }
    }
    catch(IOException e) {
      e.printStackTrace();  
    }
        
    //find .csv file and copy it under desired name
    String directory_name = "output_dist";
    File directory = new File(directory_name);
    File[] files = directory.listFiles();
    String csv_name = "";
    if (files != null) {
      for (File file : files) {
          csv_name = file.getName();
          System.out.println(csv_name);
        if (csv_name.endsWith(".csv")) {
          break;
        }
      }
    } else {
      System.out.println("No files found!");
    }
    File old_file = new File(directory_name + "/" + csv_name);
    File new_file = new File("./distance_per_player.csv");
    if (old_file.renameTo(new_file)) {
        System.out.println("success");
    }
    //delete directory copied locally
    for (File file:files) {
        file.delete();
    }
    directory.delete();

    spark.stop();
  }
}