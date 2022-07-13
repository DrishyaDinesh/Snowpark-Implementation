import com.snowflake.snowpark.functions._
import com.snowflake.snowpark._

object udf_test {
    def main(args:Array[String]): Unit = {
      val session = Main.create_session()
      val df = session.table("customer")

      println("Create and register an anonymous UDF.")
      val doubleUdf = udf((x: Double) => x + x)
      // Call the anonymous UDF.
      val dfWithDoubleNum = df.withColumn("doubleCost", doubleUdf(col("cost")))
      dfWithDoubleNum.show()

      println("Create and register a temporary named UDF.")
      session.udf.registerTemporary("SquaredUdf1", (x: Int) => x * x)

      // Call the named UDF, passing in the "num" column.
      // The example uses withColumn to return a DataFrame containing
      // the UDF result in a new column named "SquareCost".
      val dfWithTempUdf = df.withColumn("SquareCost", callUDF("SquaredUdf1", col("cost")))
      dfWithTempUdf.show()

      println("Create and register a permanent named UDF")
      //session.udf.registerPermanent("tripleUdf", (x: Double) => x + x + x, "mystage")

      // Call the named UDF.
      val dfWithTripleNum = df.withColumn("tripleCost", callUDF("tripleUdf", col("cost")))
      dfWithTripleNum.show()

      println("Calling system-defined SQL functions")
      val df_sysfn = session.table("customer").select(upper(col("name")))
      df_sysfn.show()

      println("Using callBuiltin function to call the system-defined function.")
      val result1 = df.select(callBuiltin("radians", col("product_id"))).collect()
      println(result1.mkString(" "))
      println("")

      println("Using builtin function to create a function object that you can use to call the system-defined function.")
      // Create a function object for the system-defined function RADIANS().
      val radians = builtin("radians")
      // Call the system-defined function RADIANS() on col1.
      val result2 = df.select(radians(col("product_id"))).collect()
      println(result2.mkString(" ,"))
      println("")

      println("Runs the scalar function 'tripleUdf' on column cost")
      val result3 = df.select(callUDF("tripleUdf", col("cost"))).collect()
      println(result3.mkString(" ,"))
      println("")

      println("Call the SQL UDTF product_by_category_id and creates a DataFrame for the output of the UDTF")
      val dfTableFunctionOutput = session.tableFunction(TableFunction("product_by_category_id"), Map("cat_id" -> lit(20)))
      dfTableFunctionOutput.show()

      println("")
      println("To call a stored procedure, we use the sql method of the Session class")
      // Get the list of the files in a stage.
      // The collect() method causes this SQL statement to be executed.
      println("")
      println("Using sql method to get the list of files in a stage")
      val dfStageFiles = session.sql("ls @myStage")
      val files = dfStageFiles.collect()
      files.foreach(println)

      println("")
      println("Using sql method to select id, category_id, name based on the condition category_id > 20 and id > 10")
      val df2 = session.sql("select id, category_id, name from sample_product_data where id > 10")
      val results = df2.filter(col("category_id") > 20 )
      results.show()

    }
}
