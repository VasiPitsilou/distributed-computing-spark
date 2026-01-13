import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;


public class Rebounds {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("JS").getOrCreate();
    // String path = "/cw/bdap/assignment2/nba_movement_data/moments/0021500548.csv";
    String path = "/data/nba_movement_data/moments";
    Dataset<Row> df = spark.read().option("header", "true").csv(path);

    //drop columns of no interest for this question (radius) for simplification. 
    df = df.drop("radius");
    df = df.filter(df.col("shot_clock").isNotNull());
    //cast columns to respective type and convert feet to meters
    df = df.withColumn("x_loc",functions.col("x_loc").cast("float").multiply(0.3048));
    df = df.withColumn("y_loc",functions.col("y_loc").cast("float").multiply(0.3048)); 
    df = df.withColumn("player_id",functions.col("player_id").cast("int"));
    df = df.withColumn("game_id",functions.col("game_id").cast("int"));    
    df = df.withColumn("game_clock",functions.col("game_clock").cast("float"));
    df = df.withColumn("shot_clock",functions.col("shot_clock").cast("float"));
    df = df.withColumn("quarter",functions.col("quarter").cast("int"));
    
    //read pbp data to obtain event id and data. Keep only rows that correspond to rebound events (EVENTMSGTYPE = 4)
    // path = "/cw/bdap/assignment2/nba_movement_data/events/0021500548.csv";
    path = "/data/nba_movement_data/events";
    Dataset<Row> df_event = spark.read().option("header", "true").csv(path);
    df_event = df_event.select(functions.col("GAME_ID"),functions.col("EVENTNUM"),functions.col("EVENTMSGTYPE"),
    functions.col("PCTIMESTRING"), functions.col("HOMEDESCRIPTION"),functions.col("VISITORDESCRIPTION"),functions.col("PLAYER1_ID"),
    functions.col("PLAYER1_TEAM_ID")).filter(functions.col("EVENTMSGTYPE").equalTo("4"));

    df_event = df_event.withColumn("GAME_ID",functions.col("GAME_ID").cast("int"));
    df_event = df_event.withColumnRenamed("GAME_ID","GAME"); //rename column to avoid ambiguities with dataset to be joined
    df_event = df_event.withColumn("EVENTNUM",functions.col("EVENTNUM").cast("int"));
    df_event = df_event.withColumn("EVENTMSGTYPE",functions.col("EVENTMSGTYPE").cast("int"));
    df_event = df_event.withColumn("PLAYER1_ID",functions.col("PLAYER1_ID").cast("int"));
    df_event = df_event.withColumn("PLAYER1_TEAM_ID",functions.col("PLAYER1_TEAM_ID").cast("int"));
    
    //put ball location data to a dataframe. We will use it to determine when a player is closest to the ball in a particular event, this will help determine the rebound position
    Dataset<Row> df_ball = df.filter(df.col("player_id").equalTo(-1)).select("game_id", "event_id", "game_clock", "x_loc","y_loc");
    
    //rename columns to avoid confusion in merging with player data
    df_ball = df_ball.withColumnRenamed("game_id","game_id_ball").withColumnRenamed("event_id","event_id_ball")
    .withColumnRenamed("game_clock","game_clock_ball").withColumnRenamed("x_loc","x_loc_ball").withColumnRenamed("y_loc","y_loc_ball");
    df = df.join(df_ball, df.col("event_id").equalTo(df_ball.col("event_id_ball"))
    .and(df.col("game_clock").equalTo(df_ball.col("game_clock_ball"))).and(df.col("game_id").equalTo(df_ball.col("game_id_ball"))),"inner");
    //drop duplicate columns
    df = df.drop("game_id_ball","event_id_ball", "game_clock_ball");

    //create column (distance_to_ball) containing Euclidean distance between player and ball at that particular timestamp.
    Column newcol_x = functions.col("x_loc").minus(functions.col("x_loc_ball"));
    newcol_x = functions.when(newcol_x.isNull(),0.0).otherwise(newcol_x.multiply(newcol_x));
    Column newcol_y = functions.col("y_loc").minus(functions.col("y_loc_ball"));
    newcol_y = functions.when(newcol_y.isNull(),0.0).otherwise(newcol_y.multiply(newcol_y));
    Column distance = functions.sqrt(newcol_x.plus(newcol_y));

    //measure distance to ball in instances of interest. Instances of interest are candidate rebound locations, i.e. when shot clock is 24 or higher than previous and next instance, meaning it reset
    df = df.withColumn("distance_to_ball",functions.when(functions.col("shot_clock").equalTo(24.0)
    .or(functions.col("shot_clock").gt(functions.lag(functions.col("shot_clock"),1).over(Window.partitionBy("game_id","event_id","player_id")
    .orderBy(functions.col("game_clock").desc())))
    .and(functions.col("shot_clock").gt(functions.lag(functions.col("shot_clock"),-1).over(Window.partitionBy("game_id","event_id","player_id")
    .orderBy(functions.col("game_clock").desc()))))), distance).otherwise(null));

    df = df.drop("x_loc_ball","y_loc_ball");

    //convert time string to seconds (e.g. 12:20 becomes 12*60 + 20 = 740) in events data
    Column regex_secs = functions.lit(":.*");
    Column regex_mins = functions.lit(".*:");
    Column repl = functions.lit("");
    //isolate minutes (e.g. 12 in 12:20)
    df_event = df_event.withColumn("PCTIMEMIN", functions.regexp_replace(functions.col("PCTIMESTRING"), regex_secs, repl).cast("int"));
    //isolate seconds (e.g. 20 in 12:20)
    df_event = df_event.withColumn("PCTIMESEC", functions.regexp_replace(functions.col("PCTIMESTRING"), regex_mins, repl).cast("int"));
    //convert to seconds
    df_event = df_event.withColumn("PCTIMESEC",df_event.col("PCTIMEMIN").multiply(60.0).plus(df_event.col("PCTIMESEC"))).drop(df_event.col("PCTIMEMIN"));
    
    //discard invalid rebound descriptions and join pbp with tracking data
    df_event = df_event.filter(functions.not(df_event.col("HOMEDESCRIPTION").isNull()).or(functions.not(df_event.col("VISITORDESCRIPTION").isNull())));
    //aggregate HOMEDESCRIPTION and VISITORDESCRIPTION columns into HOMEDESCRIPTION
    df_event = df_event.withColumn("HOMEDESCRIPTION",functions.when(df_event.col("HOMEDESCRIPTION").isNull(),df_event.col("VISITORDESCRIPTION")).otherwise(df_event.col("HOMEDESCRIPTION")));

     //join tracking data with event data
    df_event = df.join(df_event,df.col("event_id").equalTo(df_event.col("EVENTNUM"))
    .and(df.col("player_id").equalTo(df_event.col("PLAYER1_ID"))).and(df.col("game_id").equalTo(df_event.col("GAME"))),"inner");
    df_event = df_event.drop("EVENTNUM", "PLAYER1_ID", "PLAYER1_TEAM_ID", "PCTIMESTRING", "GAME","VISITORDESCRIPTION","EVENTMSGTYPE", "quarter");
    df_event = df_event.orderBy(df_event.col("game_id"), df_event.col("event_id"), df_event.col("game_clock").desc());
    df_event = df_event.dropDuplicates();


    //position of two baskets, in meters (y dimension same for both). Basket positions are taken from https://ak-static.cms.nba.com/wp-content/uploads/sites/4/2019/02/NBA-Court-Dimensions-.png
    Column x_loc_basket1 = functions.lit(1.65);
    Column x_loc_basket2 = functions.lit(27.0);
    Column y_loc_basket = functions.lit(7.62);

    //create column (distance_to_basket1) containing Euclidean distance between player and first basket.
    newcol_x = functions.col("x_loc").minus(x_loc_basket1);
    newcol_x = functions.when(newcol_x.isNull(),0.0).otherwise(newcol_x.multiply(newcol_x));
    newcol_y = functions.col("y_loc").minus(y_loc_basket);
    newcol_y = functions.when(newcol_y.isNull(),0.0).otherwise(newcol_y.multiply(newcol_y));
    
    Column distance_to_basket1 = functions.sqrt(newcol_x.plus(newcol_y));

    //create same column for second basket
    newcol_x = functions.col("x_loc").minus(x_loc_basket2);
    newcol_x = functions.when(newcol_x.isNull(),0.0).otherwise(newcol_x.multiply(newcol_x));
    Column distance_to_basket2 = functions.sqrt(newcol_x.plus(newcol_y));
    
    //find smaller distance of two baskets: this will tell us to which of the two baskets the rebound occured
    Column distance_closest_basket = functions.least(functions.col("distance_to_basket1"),functions.col("distance_to_basket2"));
    df_event = df_event.withColumn("distance_to_basket1",distance_to_basket1).withColumn("distance_to_basket2",distance_to_basket2);
    df_event = df_event.withColumn("distance_to_closest_basket",functions.when(df_event.col("distance_to_ball").isNotNull(),distance_closest_basket).otherwise(null));
    df_event = df_event.drop("distance_to_basket1","distance_to_basket2");

    //set distance to ball to infinity insteaad of NULL, if this is not a rebound position. this will be helpful in cases where no candidate rebound position will be found for an event
    //(if distance_to_ball was set to NULL it would be discarded in groupBy of line 127, so we put a symbolic large value instead of NULL)
    df_event = df_event.withColumn("distance_to_ball", functions.when(df_event.col("distance_to_ball").isNotNull(), df_event.col("distance_to_ball")).otherwise(Double.POSITIVE_INFINITY));
    
    //rebound timestamp is considered the one of the valid ones in which the player is closest to the ball
    Dataset<Row> final_df = df_event.groupBy("game_id","player_id", "event_id").agg(functions.min("distance_to_ball")); 
    final_df = final_df.withColumnRenamed("game_id","game_id_final").withColumnRenamed("player_id","player_id_final").withColumnRenamed("event_id","event_id_final");

    //do this join to get distance to closest basket, because the column was dropped in previous step
    final_df = final_df.join(df_event,df_event.col("game_id").equalTo(final_df.col("game_id_final"))
    .and(df_event.col("event_id").equalTo(final_df.col("event_id_final"))).and(df_event.col("distance_to_ball").equalTo(final_df.col("min(distance_to_ball)"))), "inner");
    final_df = final_df.drop("min(distance_to_ball)", "game_id_final", "player_id_final", "event_id_final", "team_id",  "x_loc", "y_loc", "game_clock", "shot_clock", "PCTIMESEC");
    final_df = final_df.dropDuplicates();

    //isolate offensive and defensive rebound index from description. The max index for each offensive/defensive rebound indicates how many of them occured
    Column regex_off = functions.lit(".*?Off:|Def:.*");
    Column regex_def = functions.lit(".*Def:|\\)");
    repl = functions.lit("");
    final_df = final_df.withColumn("no_offensive", functions.regexp_replace(functions.col("HOMEDESCRIPTION"), regex_off, repl).cast("int"))
    .withColumn("no_defensive", functions.regexp_replace(functions.col("HOMEDESCRIPTION"), regex_def, repl).cast("int"));

    //find maximum offensive and defensive rebound index, as well as distance
    final_df = final_df.groupBy("game_id", "player_id").agg(functions.max("no_offensive").alias("nb_offensive_rebounds")
    ,functions.max("no_defensive").alias("nb_defensive_rebounds"),functions.max("distance_to_closest_basket").alias("distance_to_closest_basket"));
    
    //aggregate over all included matches
    final_df = final_df.groupBy("player_id").agg(functions.sum("nb_offensive_rebounds").cast("int").alias("nb_offensive_rebounds"),
    functions.sum("nb_defensive_rebounds").cast("int").alias("nb_defensive_rebounds"),functions.max("distance_to_closest_basket").cast("float").alias("dist_farthest_rebound"));

    //round distance to 2 decimals
    final_df = final_df.withColumn("dist_farthest_rebound",functions.round(final_df.col("dist_farthest_rebound"), 2));

    //write final csv. Please note that if no valid rebound position was found for a player
    //(there was no event for him where shot_clock set to 24 or was higher than shot_clock of previous and next timestamp)
    //then the distance_from_farthest_basket will be NULL and will appear so in the rebounds.csv file
    Dataset<Row> df_write = final_df.select("player_id", "nb_offensive_rebounds", "nb_defensive_rebounds", "dist_farthest_rebound");
    df_write.write().mode("overwrite").option("delimiter"," ").option("header","true").csv("./output_rebounds_lom");

    //access dfs directory and copy it locally
    Configuration conf = new Configuration();
    Path inputPath = new Path("/user/r1028141/output_rebounds_lom");
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
    String directory_name = "output_rebounds_lom";
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
    File new_file = new File("rebounds.csv");
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