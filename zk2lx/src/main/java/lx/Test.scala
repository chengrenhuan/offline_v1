package lx
// outhor Eric
//date 9/25
object Test {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setJobDescription("error")
    val goods = spark.read
      .schema("goods_id STRING, order_id STRING, goods_name STRING, weight DOUBLE, volume DOUBLE, category STRING")
      .option("sep", ",")
      .csv("data/goods.txt")
    val logistics = spark.read
      .schema("logistics_id STRING, order_id STRING, express_company STRING, departure_time TIMESTAMP, arrival_time TIMESTAMP, current_city STRING,logistics_status STRING")
      .option("sep", ",")
      .csv("data/logistics.txt")
    val orders = spark.read
      .schema("order_id STRING, user_id STRING, order_time TIMESTAMP, total_amount DOUBLE, status STRING, origin STRING, destination STRING")
      .option("sep", ",")
      .csv("data/orders.txt")
    val users = spark.read
      .schema("user_id STRING, user_name STRING, register_time TIMESTAMP, user_level STRING, city STRING")
      .option("sep", ",")
      .csv("data/users.txt")
    goods.createOrReplaceTempView("goods")
    logistics.createOrReplaceTempView("logistics")
    orders.createOrReplaceTempView("orders")
    users.createOrReplaceTempView("users")
    val sql2 =
      """
        |select
        |    date_format(order_time,'yyyy-MM') as mode,
        |    sum(total_amount)
        |from orders
        |where year(order_time)=2023
        |group by mode
        |having sum(total_amount)>1000000
        |""".stripMargin
    val sqlDF2 = spark.sql(sql2)
    sqlDF2.show()

    val sql3 =
      """
        |select *
        |from orders o
        |join users u on o.user_id = u.user_id
        |where status=="已取消"
        |""".stripMargin
    val sqlDF3 = spark.sql(sql3)
    sqlDF3.show()

    val sql4 =
      """
        |select
        |    (unix_timestamp(arrival_time)-unix_timestamp(departure_time) )as sj
        |from logistics
        |order by sj desc limit 10;
        |""".stripMargin
    val sqlDF4 = spark.sql(sql4)
    sqlDF4.show()


    val sql5 =
      """
        |select
        |    *
        |from orders o
        |join logistics l on o.order_id = l.order_id
        |where o.origin="北京" and o.destination="上海"
        |and (unix_timestamp(l.arrival_time)-unix_timestamp(o.order_time))/3600>48
        |""".stripMargin
    val sqlDF5 = spark.sql(sql5)
    sqlDF5.show()

    val sql6 =
      """
        |select t.day,t.tiat,count(order_id)
        |from (select to_date(order_time) as day,
        |             order_id,
        |             case
        |                 when hour(order_time) between 0 and 6 then "0-6点"
        |                 when hour(order_time) between 7 and 12 then "7-12点"
        |                 when hour(order_time) between 13 and 18 then "13-18点"
        |                 when hour(order_time) between 19 and 23 then "19-23点"
        |                 end             as tiat
        |      from orders
        |      where order_time is not null)t
        |group by t.day,t.tiat
        |order by t.day,
        |         case t.tiat
        |         when "0-6点" then 1
        |         when "7-12点" then 2
        |         when "13-18点" then 3
        |         when "19-23点" then 4
        |end ;
        |""".stripMargin
    val sqlDF6 = spark.sql(sql6)
    sqlDF6.show()

  }

}
