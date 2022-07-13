import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object Main {
  def create_session(): Session = {
    // Replace the <placeholders> below.
    val configs = Map (
      "URL" -> "https://ug43226.central-india.azure.snowflakecomputing.com:443",
      "USER" -> "drishyadinesh",
      "PASSWORD" -> "Dd#15112001",
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
