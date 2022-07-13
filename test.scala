import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object test {
  def main(args:Array[String]): Unit = {
    val session = Main.create_session()

    println("Printing tableSchema of Sample_Product_Data")
    val tableSchema = session.table("sample_product_data").schema
    println("Schema for sample_product_data: " + tableSchema)

    println("Creating DataFrame from sample_product_data table")
    val df = session.table("sample_product_data")
    //Printing first 8 rows of the DataFrame
    df.show(8)

    println("Creating DataFrame from sql query")
    val df_sql=session.sql("Select * from sample_product_data")
    df_sql.show()

    println("Selecting rows whose key value is 1")
    val df_key_1 = df.filter(col("key")=== 1)
    df_key_1.show()

    println("Selecting specific columns")
    val df_cols = df.select(col("id"),col("category_id"),col("name"),(col("key")*100) as "new_key")
    df_cols.show()

    println("Creating DataFrame from customer table")
    val df_cust = session.table("customer")
    df_cust.show()

    println("Performing Join of two DataFrames")
    val dfjoined = df_cols.join(df_cust,df_cust("product_id") === df("id"))
    dfjoined.show()

    println("Sorting the DataFrame based on parent_id")
    val df_sorted = df.sort(col("parent_id"))
    df_sorted.show(15)

    println("------***********************************************************************************************************************************************************------")
    println("")
    println("Performing a Synchronous action. These actions will evaluate the DataFrame and send the SQL statement to the server for execution")
    println("Retrieving Data using collect method")
    val df2= df_sorted.collect()
    print(df2.mkString("Array(", ", ", ")"))

    println("")
    println("Retrieving Data using toLocalIterator method")
    val rowIterator = df_sorted.toLocalIterator
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      println(s"Name: ${row.getString(3)}; Category ID: ${row.getInt(2)}")
    }

    //for (i<-df_sorted.toLocalIterator){
     // print(i)
    //}
    println("Count of rows with parent_id = 0")
    val df3 = df.filter(col("parent_id")===0)
    println("Number of rows selected: "+df3.count())

    println("Caching result in the memory")
    val df_cacheresult = df3.cacheResult()
    df_cacheresult.show()

    println("Saving the data in a DataFrame into the table Sample_write")
    //df_cols.write.saveAsTable("sample_write")
    println("Data saved in the table")

    println("Deleting certain rows based on a condition")
    val delcount1 = df.delete(col("category_id")===10)
    println("Number of rows deleted: "+delcount1.rowsDeleted)

    println("Deleting certain rows in 1 DataFrame with respect to a condition on another DataFrame")
    val delcount2 = df.delete(df_key_1("category_id")===df("category_id"),df_key_1)
    println("Number of rows deleted: "+delcount2.rowsDeleted)

    println("Updating the columne value '3rd' in the table")
    val updateResult = df.update(Map("3rd" -> lit(87)))
    println(s"Number of rows updated: ${updateResult.rowsUpdated}")

    println("------***********************************************************************************************************************************************************------")
    println("")
    println("Performing a Asynchronous action using async object")
    // Create a DataFrame with the "id" and "name" columns from the "sample_product_data" table.
    // This does not execute the query.
    val df_async= session.table("sample_product_data").select(col("id"), col("name"))
    df_async.show()
    // Execute the query asynchronously.
    val asyncJob = df_async.async.collect()
    // Check if the query has completed execution.
    println(s"Is query ${asyncJob.getQueryId()} done? ${asyncJob.isDone()}")
    // Get an Array of Rows containing the results, and print the results.
    val results = asyncJob.getResult()
    results.foreach(println)
   // To execute the query asynchronously and retrieve the number of results
    val asyncJob2 = df_async.async.count()
    // Check if the query has completed execution.
    println("")
    println("Counting the Number of rows returned")
    println(s"Is query ${asyncJob2.getQueryId()} done? ${asyncJob2.isDone()}")

    // Print the count of rows in the table.
    println("Rows returned: " + asyncJob2.getResult())
    println("------***********************************************************************************************************************************************************------")

    println("")


  }
}
