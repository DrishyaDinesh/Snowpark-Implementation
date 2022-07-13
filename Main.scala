import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object Main {
  def create_session(): Session = {
    // Replace the <placeholders> below.
    val configs = Map (
      "URL" -> "https://######.#############.snowflakecomputing.com:443",
      "USER" -> "#############",
      "PASSWORD" -> "############",
      "ROLE" -> "ACCOUNTADMIN",
      "WAREHOUSE" -> "DEMO_WH",
      "DB" -> "DEMO_DB",
      "SCHEMA" -> "PUBLIC"
    )
    val session = Session.builder.configs(configs).create
    session.sql("show tables").show()

    session
  }
}
