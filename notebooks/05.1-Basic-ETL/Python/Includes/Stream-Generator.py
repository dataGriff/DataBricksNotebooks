# Databricks notebook source
class DummyDataGenerator:
  streamDirectory = "dbfs:/tmp/{}/new-flights".format(username)

None # suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import scala.util.Random
# MAGIC import java.io._
# MAGIC import java.time._
# MAGIC 
# MAGIC // Notebook #2 has to set this to 8, we are setting
# MAGIC // it to 200 to "restore" the default behavior.
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 200)
# MAGIC 
# MAGIC // Make the username available to all other languages.
# MAGIC // "WARNING: use of the "current" username is unpredictable
# MAGIC // when multiple users are collaborating and should be replaced
# MAGIC // with the notebook ID instead.
# MAGIC val username = com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.BaseTagDefinitions.TAG_USER);
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC 
# MAGIC object DummyDataGenerator extends Runnable {
# MAGIC   var runner : Thread = null;
# MAGIC   val className = getClass().getName()
# MAGIC   val streamDirectory = s"dbfs:/tmp/$username/new-flights"
# MAGIC   val airlines = Array( ("American", 0.15), ("Delta", 0.17), ("Frontier", 0.19), ("Hawaiian", 0.21), ("JetBlue", 0.25), ("United", 0.30) )
# MAGIC 
# MAGIC   val rand = new Random(System.currentTimeMillis())
# MAGIC   var maxDuration = 3 * 60 * 1000 // default to a couple of minutes
# MAGIC 
# MAGIC   def clean() {
# MAGIC     System.out.println("Removing old files for dummy data generator.")
# MAGIC     dbutils.fs.rm(streamDirectory, true)
# MAGIC     if (dbutils.fs.mkdirs(streamDirectory) == false) {
# MAGIC       throw new RuntimeException("Unable to create temp directory.")
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   def run() {
# MAGIC     val date = LocalDate.now()
# MAGIC     val start = System.currentTimeMillis()
# MAGIC 
# MAGIC     while (System.currentTimeMillis() - start < maxDuration) {
# MAGIC       try {
# MAGIC         val dir = s"/dbfs/tmp/$username/new-flights"
# MAGIC         val tempFile = File.createTempFile("flights-", "", new File(dir)).getAbsolutePath()+".csv"
# MAGIC         val writer = new PrintWriter(tempFile)
# MAGIC 
# MAGIC         for (airline <- airlines) {
# MAGIC           val flightNumber = rand.nextInt(1000)+1000
# MAGIC           val departureTime = LocalDateTime.now().plusHours(-7)
# MAGIC           val (name, odds) = airline
# MAGIC           val test = rand.nextDouble()
# MAGIC 
# MAGIC           val delay = if (test < odds)
# MAGIC             rand.nextInt(60)+(30*odds)
# MAGIC             else rand.nextInt(10)-5
# MAGIC 
# MAGIC           println(s"- Flight #$flightNumber by $name at $departureTime delayed $delay minutes")
# MAGIC           writer.println(s""" "$flightNumber","$departureTime","$delay","$name" """.trim)
# MAGIC         }
# MAGIC         writer.close()
# MAGIC 
# MAGIC         // wait a couple of seconds
# MAGIC         Thread.sleep(rand.nextInt(5000))
# MAGIC 
# MAGIC       } catch {
# MAGIC         case e: Exception => {
# MAGIC           printf("* Processing failure: %s%n", e.getMessage())
# MAGIC           return;
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC     println("No more flights!")
# MAGIC   }
# MAGIC 
# MAGIC   def start(minutes:Int = 5) {
# MAGIC     maxDuration = minutes * 60 * 1000
# MAGIC 
# MAGIC     if (runner != null) {
# MAGIC       println("Stopping dummy data generator.")
# MAGIC       runner.interrupt();
# MAGIC       runner.join();
# MAGIC     }
# MAGIC     println(s"Running dummy data generator for $minutes minutes.")
# MAGIC     runner = new Thread(this);
# MAGIC     runner.start();
# MAGIC   }
# MAGIC 
# MAGIC   def stop() {
# MAGIC     start(0)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC DummyDataGenerator.clean()
# MAGIC 
# MAGIC displayHTML("Imported streaming logic...") // suppress output