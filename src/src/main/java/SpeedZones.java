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

public class SpeedZones {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("JS").config("sth","Sth").getOrCreate();
    // String path = "/cw/bdap/assignment2/nba_movement_data/moments/0021500548.csv";
    String path = "/data/nba_movement_data/moments";
    Dataset<Row> df = spark.read().option("header", "true").csv(path);
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
    
    //calculate speed formula
    Column diff_x = functions.col("x_loc").minus(functions.lag(functions.col("x_loc"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())));
    diff_x = functions.when(diff_x.isNull(),0.0).otherwise(diff_x.multiply(diff_x));
    Column diff_y = functions.col("y_loc").minus(functions.lag(functions.col("y_loc"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())));
    diff_y = functions.when(diff_y.isNull(),0.0).otherwise(diff_y.multiply(diff_y));
    Column speed_formula = functions.sqrt(diff_x.plus(diff_y)).divide((float)1/25);

  
    //add speed if game_clock progresses, meaning the game is running
    Column speed = functions.when(functions.col("game_clock").equalTo(functions.lag(functions.col("game_clock"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc()))), -1.0).otherwise(speed_formula);
    df = df.withColumn("speed",speed);
    df = df.filter(functions.col("speed").gt(-0.01));
    String []arr = {"speed"};
    df = df.na().fill(0.0,arr);

    //calculate moving average filter
    Column moving_avg_speed = functions.col("speed");
    int i;
    for (i = 1; i <= 9; i++) {
      moving_avg_speed = moving_avg_speed.plus(functions.lag(functions.col("speed"),-i).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())));
    }
    moving_avg_speed = moving_avg_speed.divide(10);
    
    df = df.withColumn("moving_Avg_speed",moving_avg_speed);
    //filter speeds higher than 12
    df = df.filter(df.col("moving_Avg_speed").lt(12.0));
    //add column of distance travelled
    Column distance = functions.sqrt(diff_x.plus(diff_y));
    df = df.withColumn("distance", distance);


    //classify speeds into zones
    //"null" is for the last 9 instances of a player in a specific game and quarter where moving average filter is not computed
    Column speed_zone = functions.when(functions.col("moving_avg_speed").isNull(),"null").otherwise(functions.when(functions.col("moving_Avg_speed").lt(2.0),"slow").otherwise(functions.when(functions.col("moving_Avg_speed").lt(8),"normal").otherwise("fast")));
    df = df.withColumn("speed_zone",speed_zone);
    
    //create three columns, one for every speed zone. For every speed zone,
    //mark end of a run (e.g. transition from slow run to normal run) with 1. This will be useful to count the number of runs per speed zone.
    String arr3[] = {"normal","fast","null"};
    Column run_ends_slow = functions.when(functions.col("speed_zone").equalTo("slow").and(functions.lag(functions.col("speed_zone"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())).isin(arr3)),1).otherwise(0);
    arr3[0] = "slow";
    arr3[1] = "fast";
    Column run_ends_normal = functions.when(functions.col("speed_zone").equalTo("normal").and(functions.lag(functions.col("speed_zone"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())).isin(arr3)),1).otherwise(0);
    arr3[1] = "normal";
    Column run_ends_fast = functions.when(functions.col("speed_zone").equalTo("fast").and(functions.lag(functions.col("speed_zone"),-1).over(Window.partitionBy("game_id","quarter","player_id").orderBy(functions.col("game_clock").desc())).isin(arr3)),1).otherwise(0);
    
    df = df.withColumn("run_ends_slow",run_ends_slow).withColumn("run_ends_normal",run_ends_normal).withColumn("run_ends_fast",run_ends_fast);
    df.show(100,false);

    //count sum of runs per speed zone for every game and player
    Dataset<Row> byplayer = df.groupBy("game_id","quarter","player_id").agg(functions.sum("run_ends_slow").alias("slow_runs"),functions.sum("run_ends_normal").alias("normal_runs"),functions.sum("run_ends_fast").alias("fast_runs"));
    byplayer = byplayer.groupBy("player_id").agg(functions.sum("slow_runs").alias("slow_runs"), functions.sum("normal_runs").alias("normal_runs"), functions.sum("fast_runs").alias("fast_runs"));

    //aggregate results over all games
    Dataset<Row> df_dist = df.groupBy("player_id", "speed_zone").agg(functions.sum("distance").alias("zone_distance")).orderBy("player_id");
    df_dist.show(20,false);
    
    //bring dataframe to the right form for the output csv
    Dataset<Row> df1 = byplayer.select(functions.col("player_id"), functions.col("slow_runs")).withColumn("speed_zone", functions.lit("slow"));
    Dataset<Row> df2 = byplayer.select(functions.col("player_id"), functions.col("normal_runs")).withColumn("speed_zone", functions.lit("normal"));
    Dataset<Row> df3 = byplayer.select(functions.col("player_id"), functions.col("fast_runs")).withColumn("speed_zone", functions.lit("fast"));
    Dataset<Row> final_df = df1.union(df2);
    final_df = final_df.union(df3).orderBy("player_id");
    final_df.show(20,false);

    //join distance and runs dataframes. Choose left join to include all combinations of players and speed zones (in case a player did not complete a fast run, for example)
    final_df = final_df.withColumnRenamed("player_id","player").withColumnRenamed("speed_zone","speed");
    Dataset<Row> joined = final_df.join(df_dist,df_dist.col("player_id").equalTo(final_df.col("player")).and(df_dist.col("speed_zone").equalTo(final_df.col("speed"))),"left");
    arr[0] = "zone_distance";
    joined = joined.na().fill(0.0,arr);
    //round zone distance to 2 decimals
    joined = joined.withColumn("zone_distance",functions.round(functions.col("zone_distance"), 2));
    joined.orderBy("player_id").show(100, false);
    joined.orderBy("player").select(functions.col("player").alias("player_ID"),functions.col("speed").alias("speed_zone"),functions.col("slow_runs").alias("number_of_runs"),functions.col("zone_distance").alias("total_distance")).write().mode("overwrite").option("delimiter"," ").option("header","true").csv("./output_speed");

    //access dfs directory and copy it locally
    Configuration conf = new Configuration();
    Path inputPath = new Path("/user/r1028141/output_speed");
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
    String directory_name = "output_speed";
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
    File new_file = new File("./speed_zones.csv");
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