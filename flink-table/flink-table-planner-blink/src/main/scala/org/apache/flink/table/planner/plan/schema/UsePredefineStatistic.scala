package org.apache.flink.table.planner.plan.schema

object UsePredefineStatistic {
    var usePreDefine = false
    def get(): Boolean = {
      return usePreDefine
    }

    def set(boolean: Boolean): Unit = {
      usePreDefine = boolean
    }

  val q25 = "-- start query 25 in stream 0 using template query25.tpl\nselect  \n i_item_id\n ,i_item_desc\n ,s_store_id\n ,s_store_name\n ,sum(ss_net_profit) as store_sales_profit\n ,sum(sr_net_loss) as store_returns_loss\n ,sum(cs_net_profit) as catalog_sales_profit\n from\n store_sales\n ,store_returns\n ,catalog_sales\n ,date_dim d1\n ,date_dim d2\n ,date_dim d3\n ,store\n ,item\n where\n d1.d_moy = 4\n and d1.d_year = 1998\n and d1.d_date_sk = ss_sold_date_sk\n and i_item_sk = ss_item_sk\n and s_store_sk = ss_store_sk\n and ss_customer_sk = sr_customer_sk\n and ss_item_sk = sr_item_sk\n and ss_ticket_number = sr_ticket_number\n and sr_returned_date_sk = d2.d_date_sk\n and d2.d_moy               between 4 and  10\n and d2.d_year              = 1998\n and sr_customer_sk = cs_bill_customer_sk\n and sr_item_sk = cs_item_sk\n and cs_sold_date_sk = d3.d_date_sk\n and d3.d_moy               between 4 and  10 \n and d3.d_year              = 1998\n group by\n i_item_id\n ,i_item_desc\n ,s_store_id\n ,s_store_name\n order by\n i_item_id\n ,i_item_desc\n ,s_store_id\n ,s_store_name\n limit 100\n\n-- end query 25 in stream 0 using template query25.tpl";
  val q98 = "-- start query 98 in stream 0 using template query98.tpl\nselect i_item_id\n      ,i_item_desc \n      ,i_category \n      ,i_class \n      ,i_current_price\n      ,sum(ss_ext_sales_price) as itemrevenue \n      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over\n          (partition by i_class) as revenueratio\nfrom\t\n\tstore_sales\n    \t,item \n    \t,date_dim\nwhere \n\tss_item_sk = i_item_sk \n  \tand i_category in ('Electronics', 'Women', 'Men')\n  \tand ss_sold_date_sk = d_date_sk\n\tand d_date between cast('1998-01-02' as date) \n\t\t\t\tand (cast('1998-01-02' as date) + INTERVAL '30' day)\ngroup by \n\ti_item_id\n        ,i_item_desc \n        ,i_category\n        ,i_class\n        ,i_current_price\norder by \n\ti_category\n        ,i_class\n        ,i_item_id\n        ,i_item_desc\n        ,revenueratio\n\n-- end query 98 in stream 0 using template query98.tpl";
}
