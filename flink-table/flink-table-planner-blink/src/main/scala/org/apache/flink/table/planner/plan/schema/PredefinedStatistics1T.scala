package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.collection.JavaConversions._

object PredefinedStatistics1T {
  val catalog_sales = new TableStats(1439955454L, Map[String, ColumnStats](
    "cs_ship_mode_sk" -> new ColumnStats(20L, 7204203L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_bill_customer_sk" -> new ColumnStats(12013654L, 7204104L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_sales_price" -> new ColumnStats(29651L, 7201211L, 12.0D, 12, convertToNumber("300.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_net_profit" -> new ColumnStats(2054824L, 0L, 12.0D, 12, convertToNumber("19980.0",DataTypes.DECIMAL(7, 2)), convertToNumber("-10000.0",DataTypes.DECIMAL(7, 2))),
    "cs_ext_tax" -> new ColumnStats(218638L, 7204497L, 12.0D, 12, convertToNumber("2682.9",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_wholesale_cost" -> new ColumnStats(9872L, 7202094L, 12.0D, 12, convertToNumber("100.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "cs_ext_wholesale_cost" -> new ColumnStats(391162L, 7201335L, 12.0D, 12, convertToNumber("10000.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "cs_ship_hdemo_sk" -> new ColumnStats(7207L, 7202377L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_bill_addr_sk" -> new ColumnStats(5951214L, 7201420L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid_inc_ship_tax" -> new ColumnStats(3352208L, 0L, 12.0D, 12, convertToNumber("46389.84",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_ship_customer_sk" -> new ColumnStats(12013654L, 7201759L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_ext_discount_amt" -> new ColumnStats(1111329L, 7203783L, 12.0D, 12, convertToNumber("29982.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_sold_date_sk" -> new ColumnStats(1840L, 7203484L, 8.0D, 8, convertToNumber("2452654",DataTypes.BIGINT), convertToNumber("2450815",DataTypes.BIGINT)),
    "cs_order_number" -> new ColumnStats(161207707L, 0L, 8.0D, 8, convertToNumber("160000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_quantity" -> new ColumnStats(100L, 7202835L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_bill_cdemo_sk" -> new ColumnStats(1913326L, 7201281L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_call_center_sk" -> new ColumnStats(42L, 7199983L, 8.0D, 8, convertToNumber("42",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid_inc_tax" -> new ColumnStats(2441000L, 7201846L, 12.0D, 12, convertToNumber("32492.9",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_catalog_page_sk" -> new ColumnStats(16986L, 7202037L, 8.0D, 8, convertToNumber("25207",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_ship_cdemo_sk" -> new ColumnStats(1913326L, 7201622L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_warehouse_sk" -> new ColumnStats(20L, 7203732L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid" -> new ColumnStats(1814584L, 7198106L, 12.0D, 12, convertToNumber("29970.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_ext_list_price" -> new ColumnStats(1173301L, 7202148L, 12.0D, 12, convertToNumber("30000.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "cs_list_price" -> new ColumnStats(29854L, 7202891L, 12.0D, 12, convertToNumber("300.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "cs_promo_sk" -> new ColumnStats(1507L, 7203590L, 8.0D, 8, convertToNumber("1500",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_ship_addr_sk" -> new ColumnStats(5951214L, 7202872L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid_inc_ship" -> new ColumnStats(2552910L, 0L, 12.0D, 12, convertToNumber("44263.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_ext_sales_price" -> new ColumnStats(1110450L, 7202155L, 12.0D, 12, convertToNumber("29970.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_ship_date_sk" -> new ColumnStats(1932L, 7202108L, 8.0D, 8, convertToNumber("2452744",DataTypes.BIGINT), convertToNumber("2450817",DataTypes.BIGINT)),
    "cs_ext_ship_cost" -> new ColumnStats(571121L, 7205621L, 12.0D, 12, convertToNumber("14950.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_coupon_amt" -> new ColumnStats(1587584L, 7198730L, 12.0D, 12, convertToNumber("28824.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cs_bill_hdemo_sk" -> new ColumnStats(7207L, 7202207L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_sold_time_sk" -> new ColumnStats(86180L, 7200618L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT))))
  val catalog_returns = new TableStats(143986586L, Map[String, ColumnStats](
    "cr_order_number" -> new ColumnStats(94516461L, 0L, 8.0D, 8, convertToNumber("160000000",DataTypes.BIGINT), convertToNumber("2",DataTypes.BIGINT)),
    "cr_return_amount" -> new ColumnStats(899930L, 2877870L, 12.0D, 12, convertToNumber("29970.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_refunded_addr_sk" -> new ColumnStats(5951214L, 2878367L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_net_loss" -> new ColumnStats(909371L, 2877798L, 12.0D, 12, convertToNumber("16246.27",DataTypes.DECIMAL(7, 2)), convertToNumber("0.5",DataTypes.DECIMAL(7, 2))),
    "cr_return_amt_inc_tax" -> new ColumnStats(1562373L, 2880790L, 12.0D, 12, convertToNumber("31188.14",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_catalog_page_sk" -> new ColumnStats(16986L, 2881672L, 8.0D, 8, convertToNumber("25207",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returning_customer_sk" -> new ColumnStats(12013654L, 2878634L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_store_credit" -> new ColumnStats(788939L, 2876688L, 12.0D, 12, convertToNumber("23399.71",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_returning_addr_sk" -> new ColumnStats(5951214L, 2881305L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returning_hdemo_sk" -> new ColumnStats(7207L, 2881363L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_fee" -> new ColumnStats(9919L, 2879112L, 12.0D, 12, convertToNumber("100.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.5",DataTypes.DECIMAL(7, 2))),
    "cr_refunded_cash" -> new ColumnStats(1097347L, 2879620L, 12.0D, 12, convertToNumber("27235.18",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_return_ship_cost" -> new ColumnStats(479929L, 2879458L, 12.0D, 12, convertToNumber("14334.46",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_returned_time_sk" -> new ColumnStats(86180L, 0L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cr_return_tax" -> new ColumnStats(151309L, 2878926L, 12.0D, 12, convertToNumber("2527.54",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_refunded_customer_sk" -> new ColumnStats(12009470L, 2879362L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_ship_mode_sk" -> new ColumnStats(20L, 2879621L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_refunded_cdemo_sk" -> new ColumnStats(1913326L, 2880965L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returned_date_sk" -> new ColumnStats(2110L, 0L, 8.0D, 8, convertToNumber("2452924",DataTypes.BIGINT), convertToNumber("2450821",DataTypes.BIGINT)),
    "cr_reversed_charge" -> new ColumnStats(803117L, 2877801L, 12.0D, 12, convertToNumber("26322.64",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "cr_call_center_sk" -> new ColumnStats(42L, 2876707L, 8.0D, 8, convertToNumber("42",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_refunded_hdemo_sk" -> new ColumnStats(7207L, 2876997L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_warehouse_sk" -> new ColumnStats(20L, 2879092L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returning_cdemo_sk" -> new ColumnStats(1913326L, 2878998L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_return_quantity" -> new ColumnStats(100L, 2878978L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_reason_sk" -> new ColumnStats(65L, 2877590L, 8.0D, 8, convertToNumber("65",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))
  val inventory = new TableStats(783000000L, Map[String, ColumnStats](
    "inv_date_sk" -> new ColumnStats(261L, 0L, 8.0D, 8, convertToNumber("2452635",DataTypes.BIGINT), convertToNumber("2450815",DataTypes.BIGINT)),
    "inv_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "inv_warehouse_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "inv_quantity_on_hand" -> new ColumnStats(1003L, 39153219L, 4.0D, 4, convertToNumber("1000",DataTypes.INT), convertToNumber("0",DataTypes.INT))))
  val store_sales = new TableStats(2880064491L, Map[String, ColumnStats](
    "ss_quantity" -> new ColumnStats(100L, 129613509L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_wholesale_cost" -> new ColumnStats(9872L, 129587886L, 12.0D, 12, convertToNumber("100.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ss_net_paid" -> new ColumnStats(1296753L, 129605816L, 12.0D, 12, convertToNumber("19996.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_net_profit" -> new ColumnStats(1518008L, 129590738L, 12.0D, 12, convertToNumber("9998.0",DataTypes.DECIMAL(7, 2)), convertToNumber("-10000.0",DataTypes.DECIMAL(7, 2))),
    "ss_net_paid_inc_tax" -> new ColumnStats(1689754L, 129617068L, 12.0D, 12, convertToNumber("21571.1",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_sales_price" -> new ColumnStats(19720L, 129610852L, 12.0D, 12, convertToNumber("200.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_ticket_number" -> new ColumnStats(238013713L, 0L, 8.0D, 8, convertToNumber("240000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_ext_wholesale_cost" -> new ColumnStats(391162L, 129599766L, 12.0D, 12, convertToNumber("10000.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ss_cdemo_sk" -> new ColumnStats(1913326L, 129607785L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_promo_sk" -> new ColumnStats(1507L, 129600667L, 8.0D, 8, convertToNumber("1500",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_sold_date_sk" -> new ColumnStats(1826L, 129606894L, 8.0D, 8, convertToNumber("2452642",DataTypes.BIGINT), convertToNumber("2450816",DataTypes.BIGINT)),
    "ss_ext_sales_price" -> new ColumnStats(758040L, 129596342L, 12.0D, 12, convertToNumber("19996.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_sold_time_sk" -> new ColumnStats(46629L, 129604004L, 8.0D, 8, convertToNumber("75599",DataTypes.BIGINT), convertToNumber("28800",DataTypes.BIGINT)),
    "ss_customer_sk" -> new ColumnStats(12013654L, 129624236L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_list_price" -> new ColumnStats(19607L, 129604979L, 12.0D, 12, convertToNumber("200.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ss_store_sk" -> new ColumnStats(500L, 129594746L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_addr_sk" -> new ColumnStats(5951214L, 129604856L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_ext_discount_amt" -> new ColumnStats(1169109L, 129626909L, 12.0D, 12, convertToNumber("19327.56",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_coupon_amt" -> new ColumnStats(1169109L, 129626909L, 12.0D, 12, convertToNumber("19327.56",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_ext_tax" -> new ColumnStats(151465L, 129595176L, 12.0D, 12, convertToNumber("1781.1",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ss_hdemo_sk" -> new ColumnStats(7207L, 129596450L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_ext_list_price" -> new ColumnStats(777225L, 129598432L, 12.0D, 12, convertToNumber("20000.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2)))))
  val store_returns = new TableStats(287999847L, Map[String, ColumnStats](
    "sr_return_time_sk" -> new ColumnStats(32528L, 10080725L, 8.0D, 8, convertToNumber("61199",DataTypes.BIGINT), convertToNumber("28799",DataTypes.BIGINT)),
    "sr_return_ship_cost" -> new ColumnStats(353132L, 10082563L, 12.0D, 12, convertToNumber("9964.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "sr_cdemo_sk" -> new ColumnStats(1913326L, 10079754L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_store_credit" -> new ColumnStats(691200L, 10079289L, 12.0D, 12, convertToNumber("17462.86",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "sr_return_amt_inc_tax" -> new ColumnStats(1264856L, 10079774L, 12.0D, 12, convertToNumber("20897.04",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "sr_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_return_quantity" -> new ColumnStats(100L, 10081063L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_net_loss" -> new ColumnStats(710193L, 10078606L, 12.0D, 12, convertToNumber("10829.59",DataTypes.DECIMAL(7, 2)), convertToNumber("0.5",DataTypes.DECIMAL(7, 2))),
    "sr_ticket_number" -> new ColumnStats(169293815L, 0L, 8.0D, 8, convertToNumber("240000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_refunded_cash" -> new ColumnStats(930108L, 10082652L, 12.0D, 12, convertToNumber("18734.66",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "sr_hdemo_sk" -> new ColumnStats(7207L, 10080590L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_fee" -> new ColumnStats(9919L, 10077970L, 12.0D, 12, convertToNumber("100.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.5",DataTypes.DECIMAL(7, 2))),
    "sr_addr_sk" -> new ColumnStats(5951214L, 10080836L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_returned_date_sk" -> new ColumnStats(2007L, 10078586L, 8.0D, 8, convertToNumber("2452822",DataTypes.BIGINT), convertToNumber("2450820",DataTypes.BIGINT)),
    "sr_store_sk" -> new ColumnStats(500L, 10084534L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_return_tax" -> new ColumnStats(117519L, 10079757L, 12.0D, 12, convertToNumber("1672.66",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "sr_customer_sk" -> new ColumnStats(12013654L, 10083542L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_reason_sk" -> new ColumnStats(65L, 10081959L, 8.0D, 8, convertToNumber("65",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_reversed_charge" -> new ColumnStats(694138L, 10083930L, 12.0D, 12, convertToNumber("17310.59",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "sr_return_amt" -> new ColumnStats(676592L, 10076567L, 12.0D, 12, convertToNumber("19349.12",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2)))))
  val web_sales = new TableStats(719991178L, Map[String, ColumnStats](
    "ws_ship_mode_sk" -> new ColumnStats(20L, 179862L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_paid_inc_ship" -> new ColumnStats(2462301L, 0L, 12.0D, 12, convertToNumber("44110.44",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_coupon_amt" -> new ColumnStats(1508814L, 179930L, 12.0D, 12, convertToNumber("28824.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_ext_discount_amt" -> new ColumnStats(1102178L, 179945L, 12.0D, 12, convertToNumber("29982.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_order_number" -> new ColumnStats(60474089L, 0L, 8.0D, 8, convertToNumber("60000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ext_list_price" -> new ColumnStats(1173301L, 179814L, 12.0D, 12, convertToNumber("30000.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ws_bill_customer_sk" -> new ColumnStats(11954853L, 180319L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_promo_sk" -> new ColumnStats(1507L, 179780L, 8.0D, 8, convertToNumber("1500",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_profit" -> new ColumnStats(2003146L, 0L, 12.0D, 12, convertToNumber("19980.0",DataTypes.DECIMAL(7, 2)), convertToNumber("-10000.0",DataTypes.DECIMAL(7, 2))),
    "ws_net_paid" -> new ColumnStats(1758435L, 179640L, 12.0D, 12, convertToNumber("29970.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_paid_inc_ship_tax" -> new ColumnStats(3246392L, 0L, 12.0D, 12, convertToNumber("46389.84",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_web_page_sk" -> new ColumnStats(2992L, 180080L, 8.0D, 8, convertToNumber("3000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_warehouse_sk" -> new ColumnStats(20L, 179824L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_cdemo_sk" -> new ColumnStats(1913326L, 179732L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_sold_date_sk" -> new ColumnStats(1826L, 179939L, 8.0D, 8, convertToNumber("2452642",DataTypes.BIGINT), convertToNumber("2450816",DataTypes.BIGINT)),
    "ws_list_price" -> new ColumnStats(29854L, 180078L, 12.0D, 12, convertToNumber("300.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ws_sold_time_sk" -> new ColumnStats(86180L, 180226L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "ws_ext_ship_cost" -> new ColumnStats(567458L, 180095L, 12.0D, 12, convertToNumber("14950.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_quantity" -> new ColumnStats(100L, 179948L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_hdemo_sk" -> new ColumnStats(7207L, 180178L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ext_tax" -> new ColumnStats(213817L, 180077L, 12.0D, 12, convertToNumber("2682.9",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_sales_price" -> new ColumnStats(29645L, 179778L, 12.0D, 12, convertToNumber("300.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_ext_wholesale_cost" -> new ColumnStats(391162L, 179875L, 12.0D, 12, convertToNumber("10000.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ws_bill_cdemo_sk" -> new ColumnStats(1913326L, 179888L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_paid_inc_tax" -> new ColumnStats(2384678L, 179549L, 12.0D, 12, convertToNumber("32492.9",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_ship_addr_sk" -> new ColumnStats(5949790L, 179823L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_bill_addr_sk" -> new ColumnStats(5951214L, 180107L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_date_sk" -> new ColumnStats(1952L, 180220L, 8.0D, 8, convertToNumber("2452762",DataTypes.BIGINT), convertToNumber("2450817",DataTypes.BIGINT)),
    "ws_bill_hdemo_sk" -> new ColumnStats(7207L, 180276L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_customer_sk" -> new ColumnStats(11938839L, 180012L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ext_sales_price" -> new ColumnStats(1101246L, 179704L, 12.0D, 12, convertToNumber("29970.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "ws_wholesale_cost" -> new ColumnStats(9872L, 179926L, 12.0D, 12, convertToNumber("100.0",DataTypes.DECIMAL(7, 2)), convertToNumber("1.0",DataTypes.DECIMAL(7, 2))),
    "ws_web_site_sk" -> new ColumnStats(54L, 179876L, 8.0D, 8, convertToNumber("54",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))
  val web_returns = new TableStats(71993675L, Map[String, ColumnStats](
    "wr_reason_sk" -> new ColumnStats(65L, 3240446L, 8.0D, 8, convertToNumber("65",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_fee" -> new ColumnStats(9919L, 3243134L, 12.0D, 12, convertToNumber("100.0",DataTypes.DECIMAL(7, 2)), convertToNumber("0.5",DataTypes.DECIMAL(7, 2))),
    "wr_return_amt" -> new ColumnStats(820826L, 3243369L, 12.0D, 12, convertToNumber("29213.8",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_returning_addr_sk" -> new ColumnStats(5951214L, 3241786L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_returned_time_sk" -> new ColumnStats(86180L, 3241939L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "wr_returning_cdemo_sk" -> new ColumnStats(1913326L, 3238116L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_web_page_sk" -> new ColumnStats(2992L, 3243331L, 8.0D, 8, convertToNumber("3000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_tax" -> new ColumnStats(138079L, 3240888L, 12.0D, 12, convertToNumber("2547.93",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_refunded_customer_sk" -> new ColumnStats(11982307L, 3242009L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_ship_cost" -> new ColumnStats(449969L, 3241470L, 12.0D, 12, convertToNumber("14387.67",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_refunded_hdemo_sk" -> new ColumnStats(7207L, 3238387L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_account_credit" -> new ColumnStats(676392L, 3239646L, 12.0D, 12, convertToNumber("24867.2",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_returned_date_sk" -> new ColumnStats(2193L, 3241492L, 8.0D, 8, convertToNumber("2453002",DataTypes.BIGINT), convertToNumber("2450819",DataTypes.BIGINT)),
    "wr_reversed_charge" -> new ColumnStats(683913L, 3241982L, 12.0D, 12, convertToNumber("22969.69",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_order_number" -> new ColumnStats(42109119L, 0L, 8.0D, 8, convertToNumber("59999999",DataTypes.BIGINT), convertToNumber("2",DataTypes.BIGINT)),
    "wr_returning_customer_sk" -> new ColumnStats(11983009L, 3240499L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_amt_inc_tax" -> new ColumnStats(1369179L, 3241628L, 12.0D, 12, convertToNumber("30858.33",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_returning_hdemo_sk" -> new ColumnStats(7207L, 3241961L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_quantity" -> new ColumnStats(100L, 3241958L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_refunded_cash" -> new ColumnStats(953730L, 3239265L, 12.0D, 12, convertToNumber("26328.42",DataTypes.DECIMAL(7, 2)), convertToNumber("0.0",DataTypes.DECIMAL(7, 2))),
    "wr_refunded_addr_sk" -> new ColumnStats(5951214L, 3239540L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_refunded_cdemo_sk" -> new ColumnStats(1913326L, 3241555L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_net_loss" -> new ColumnStats(812177L, 3241037L, 12.0D, 12, convertToNumber("15343.54",DataTypes.DECIMAL(7, 2)), convertToNumber("0.5",DataTypes.DECIMAL(7, 2)))))
  val call_center = new TableStats(42L, Map[String, ColumnStats](
    "cc_street_number" -> new ColumnStats(21L, 0L, 2.9285714285714284D, 3, null, null),
    "cc_call_center_id" -> new ColumnStats(21L, 0L, 16.0D, 16, null, null),
    "cc_state" -> new ColumnStats(13L, 0L, 2.0D, 2, null, null),
    "cc_tax_percentage" -> new ColumnStats(9L, 0L, 12.0D, 12, convertToNumber("0.12",DataTypes.DECIMAL(5, 2)), convertToNumber("0.02",DataTypes.DECIMAL(5, 2))),
    "cc_division_name" -> new ColumnStats(6L, 0L, 3.738095238095238D, 5, null, null),
    "cc_hours" -> new ColumnStats(3L, 0L, 7.142857142857143D, 8, null, null),
    "cc_manager" -> new ColumnStats(35L, 0L, 13.142857142857142D, 18, null, null),
    "cc_name" -> new ColumnStats(21L, 0L, 13.619047619047619D, 19, null, null),
    "cc_employees" -> new ColumnStats(29L, 0L, 8.0D, 8, convertToNumber("6877799",DataTypes.BIGINT), convertToNumber("691766",DataTypes.BIGINT)),
    "cc_mkt_id" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cc_class" -> new ColumnStats(3L, 0L, 5.261904761904762D, 6, null, null),
    "cc_division" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cc_street_name" -> new ColumnStats(21L, 0L, 10.19047619047619D, 17, null, null),
    "cc_rec_end_date" -> new ColumnStats(3L, 21L, 12.0D, 12, null, null),
    "cc_gmt_offset" -> new ColumnStats(2L, 0L, 12.0D, 12, convertToNumber("-5.0",DataTypes.DECIMAL(5, 2)), convertToNumber("-6.0",DataTypes.DECIMAL(5, 2))),
    "cc_market_manager" -> new ColumnStats(30L, 0L, 13.476190476190476D, 18, null, null),
    "cc_open_date_sk" -> new ColumnStats(21L, 0L, 8.0D, 8, convertToNumber("2451136",DataTypes.BIGINT), convertToNumber("2450810",DataTypes.BIGINT)),
    "cc_country" -> new ColumnStats(1L, 0L, 13.0D, 13, null, null),
    "cc_closed_date_sk" -> new ColumnStats(0L, 42L, 8.0D, 8, null, null),
    "cc_company_name" -> new ColumnStats(6L, 0L, 3.857142857142857D, 5, null, null),
    "cc_city" -> new ColumnStats(16L, 0L, 7.714285714285714D, 11, null, null),
    "cc_company" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cc_county" -> new ColumnStats(15L, 0L, 14.69047619047619D, 22, null, null),
    "cc_rec_start_date" -> new ColumnStats(4L, 0L, 12.0D, 12, null, null),
    "cc_street_type" -> new ColumnStats(12L, 0L, 4.0D, 9, null, null),
    "cc_sq_ft" -> new ColumnStats(31L, 0L, 8.0D, 8, convertToNumber("2095068426",DataTypes.BIGINT), convertToNumber("-2141286656",DataTypes.BIGINT)),
    "cc_mkt_class" -> new ColumnStats(29L, 0L, 36.80952380952381D, 50, null, null),
    "cc_zip" -> new ColumnStats(21L, 0L, 5.0D, 5, null, null),
    "cc_mkt_desc" -> new ColumnStats(30L, 0L, 60.69047619047619D, 97, null, null),
    "cc_suite_number" -> new ColumnStats(16L, 0L, 7.761904761904762D, 9, null, null),
    "cc_call_center_sk" -> new ColumnStats(42L, 0L, 8.0D, 8, convertToNumber("42",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))
  val catalog_page = new TableStats(30000L, Map[String, ColumnStats](
    "cp_department" -> new ColumnStats(2L, 0L, 9.902666666666667D, 10, null, null),
    "cp_catalog_page_number" -> new ColumnStats(277L, 314L, 8.0D, 8, convertToNumber("277",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cp_catalog_page_sk" -> new ColumnStats(30053L, 0L, 8.0D, 8, convertToNumber("30000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cp_end_date_sk" -> new ColumnStats(96L, 278L, 8.0D, 8, convertToNumber("2453186",DataTypes.BIGINT), convertToNumber("2450844",DataTypes.BIGINT)),
    "cp_catalog_page_id" -> new ColumnStats(30331L, 0L, 16.0D, 16, null, null),
    "cp_description" -> new ColumnStats(29737L, 0L, 73.8035D, 99, null, null),
    "cp_type" -> new ColumnStats(4L, 0L, 7.5940666666666665D, 9, null, null),
    "cp_start_date_sk" -> new ColumnStats(91L, 297L, 8.0D, 8, convertToNumber("2453005",DataTypes.BIGINT), convertToNumber("2450815",DataTypes.BIGINT)),
    "cp_catalog_number" -> new ColumnStats(109L, 313L, 8.0D, 8, convertToNumber("109",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))
  val customer = new TableStats(12000000L, Map[String, ColumnStats](
    "c_email_address" -> new ColumnStats(11436977L, 0L, 26.49984775D, 48, null, null),
    "c_birth_month" -> new ColumnStats(12L, 421470L, 8.0D, 8, convertToNumber("12",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_first_sales_date_sk" -> new ColumnStats(3623L, 421622L, 8.0D, 8, convertToNumber("2452648",DataTypes.BIGINT), convertToNumber("2448998",DataTypes.BIGINT)),
    "c_customer_id" -> new ColumnStats(11944908L, 0L, 16.0D, 16, null, null),
    "c_birth_country" -> new ColumnStats(208L, 0L, 8.387533333333334D, 20, null, null),
    "c_salutation" -> new ColumnStats(7L, 0L, 3.1283396666666667D, 4, null, null),
    "c_last_name" -> new ColumnStats(5005L, 0L, 5.915257666666666D, 13, null, null),
    "c_birth_day" -> new ColumnStats(31L, 421587L, 8.0D, 8, convertToNumber("31",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_current_cdemo_sk" -> new ColumnStats(1907837L, 421680L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_login" -> new ColumnStats(1L, 0L, 0.0D, 0, null, null),
    "c_last_review_date" -> new ColumnStats(365L, 421965L, 8.0D, 8, convertToNumber("2452648",DataTypes.BIGINT), convertToNumber("2452283",DataTypes.BIGINT)),
    "c_first_shipto_date_sk" -> new ColumnStats(3624L, 421729L, 8.0D, 8, convertToNumber("2452678",DataTypes.BIGINT), convertToNumber("2449028",DataTypes.BIGINT)),
    "c_current_addr_sk" -> new ColumnStats(5179542L, 0L, 8.0D, 8, convertToNumber("5999998",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_birth_year" -> new ColumnStats(69L, 421081L, 8.0D, 8, convertToNumber("1992",DataTypes.BIGINT), convertToNumber("1924",DataTypes.BIGINT)),
    "c_preferred_cust_flag" -> new ColumnStats(3L, 0L, 0.9649526666666667D, 1, null, null),
    "c_current_hdemo_sk" -> new ColumnStats(7207L, 421081L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_customer_sk" -> new ColumnStats(12013654L, 0L, 8.0D, 8, convertToNumber("12000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_first_name" -> new ColumnStats(5179L, 0L, 5.63038975D, 11, null, null)))
  val customer_address = new TableStats(6000000L, Map[String, ColumnStats](
    "ca_state" -> new ColumnStats(52L, 0L, 1.9400013333333332D, 2, null, null),
    "ca_street_type" -> new ColumnStats(21L, 0L, 4.073662166666667D, 9, null, null),
    "ca_gmt_offset" -> new ColumnStats(6L, 180124L, 12.0D, 12, convertToNumber("-5.0",DataTypes.DECIMAL(5, 2)), convertToNumber("-10.0",DataTypes.DECIMAL(5, 2))),
    "ca_location_type" -> new ColumnStats(4L, 0L, 8.727550333333333D, 13, null, null),
    "ca_street_number" -> new ColumnStats(1005L, 0L, 2.8062643333333335D, 4, null, null),
    "ca_address_id" -> new ColumnStats(5967552L, 0L, 16.0D, 16, null, null),
    "ca_suite_number" -> new ColumnStats(76L, 0L, 7.653235666666666D, 9, null, null),
    "ca_country" -> new ColumnStats(2L, 0L, 12.609768166666667D, 13, null, null),
    "ca_zip" -> new ColumnStats(9381L, 0L, 4.849795833333333D, 5, null, null),
    "ca_address_sk" -> new ColumnStats(5951214L, 0L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ca_county" -> new ColumnStats(1856L, 0L, 13.541667833333333D, 28, null, null),
    "ca_city" -> new ColumnStats(991L, 0L, 8.683290666666666D, 20, null, null),
    "ca_street_name" -> new ColumnStats(8216L, 0L, 8.448978166666667D, 21, null, null)))
  val customer_demographics = new TableStats(1920800L, Map[String, ColumnStats](
    "cd_demo_sk" -> new ColumnStats(1913326L, 0L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cd_education_status" -> new ColumnStats(7L, 0L, 9.571428571428571D, 15, null, null),
    "cd_credit_rating" -> new ColumnStats(4L, 0L, 7.0D, 9, null, null),
    "cd_purchase_estimate" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("10000",DataTypes.BIGINT), convertToNumber("500",DataTypes.BIGINT)),
    "cd_dep_college_count" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cd_dep_count" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cd_gender" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "cd_dep_employed_count" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cd_marital_status" -> new ColumnStats(5L, 0L, 1.0D, 1, null, null)))
  val date_dim = new TableStats(73049L, Map[String, ColumnStats](
    "d_same_day_lq" -> new ColumnStats(72475L, 0L, 8.0D, 8, convertToNumber("2487978",DataTypes.BIGINT), convertToNumber("2414930",DataTypes.BIGINT)),
    "d_holiday" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "d_date" -> new ColumnStats(73063L, 0L, 12.0D, 12, 47482, -25566),
    "d_current_week" -> new ColumnStats(1L, 0L, 1.0D, 1, null, null),
    "d_current_day" -> new ColumnStats(1L, 0L, 1.0D, 1, null, null),
    "d_week_seq" -> new ColumnStats(10393L, 0L, 8.0D, 8, convertToNumber("10436",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "d_day_name" -> new ColumnStats(7L, 0L, 7.142863009760572D, 9, null, null),
    "d_year" -> new ColumnStats(198L, 0L, 8.0D, 8, convertToNumber("2100",DataTypes.BIGINT), convertToNumber("1900",DataTypes.BIGINT)),
    "d_date_id" -> new ColumnStats(73069L, 0L, 16.0D, 16, null, null),
    "d_first_dom" -> new ColumnStats(2394L, 0L, 8.0D, 8, convertToNumber("2488070",DataTypes.BIGINT), convertToNumber("2415021",DataTypes.BIGINT)),
    "d_moy" -> new ColumnStats(12L, 0L, 8.0D, 8, convertToNumber("12",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "d_current_month" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "d_quarter_name" -> new ColumnStats(797L, 0L, 6.0D, 6, null, null),
    "d_same_day_ly" -> new ColumnStats(72606L, 0L, 8.0D, 8, convertToNumber("2487705",DataTypes.BIGINT), convertToNumber("2414657",DataTypes.BIGINT)),
    "d_fy_year" -> new ColumnStats(198L, 0L, 8.0D, 8, convertToNumber("2100",DataTypes.BIGINT), convertToNumber("1900",DataTypes.BIGINT)),
    "d_following_holiday" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "d_weekend" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "d_fy_week_seq" -> new ColumnStats(10393L, 0L, 8.0D, 8, convertToNumber("10436",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "d_current_year" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "d_date_sk" -> new ColumnStats(72895L, 0L, 8.0D, 8, convertToNumber("2488070",DataTypes.BIGINT), convertToNumber("2415022",DataTypes.BIGINT)),
    "d_qoy" -> new ColumnStats(4L, 0L, 8.0D, 8, convertToNumber("4",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "d_dow" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "d_fy_quarter_seq" -> new ColumnStats(798L, 0L, 8.0D, 8, convertToNumber("801",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "d_quarter_seq" -> new ColumnStats(798L, 0L, 8.0D, 8, convertToNumber("801",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "d_last_dom" -> new ColumnStats(2387L, 0L, 8.0D, 8, convertToNumber("2488372",DataTypes.BIGINT), convertToNumber("2415020",DataTypes.BIGINT)),
    "d_current_quarter" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "d_month_seq" -> new ColumnStats(2399L, 0L, 8.0D, 8, convertToNumber("2400",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "d_dom" -> new ColumnStats(31L, 0L, 8.0D, 8, convertToNumber("31",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))
  val household_demographics = new TableStats(7200L, Map[String, ColumnStats](
    "hd_demo_sk" -> new ColumnStats(7207L, 0L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "hd_income_band_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "hd_dep_count" -> new ColumnStats(10L, 0L, 8.0D, 8, convertToNumber("9",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "hd_buy_potential" -> new ColumnStats(6L, 0L, 7.5D, 10, null, null),
    "hd_vehicle_count" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("4",DataTypes.BIGINT), convertToNumber("-1",DataTypes.BIGINT))))
  val income_band = new TableStats(20L, Map[String, ColumnStats](
    "ib_income_band_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ib_lower_bound" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("190001",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "ib_upper_bound" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("200000",DataTypes.BIGINT), convertToNumber("10000",DataTypes.BIGINT))))
  val item = new TableStats(300000L, Map[String, ColumnStats](
    "i_units" -> new ColumnStats(22L, 0L, 4.17872D, 7, null, null),
    "i_brand" -> new ColumnStats(709L, 0L, 16.110786666666666D, 22, null, null),
    "i_rec_start_date" -> new ColumnStats(4L, 725L, 12.0D, 12, null, null),
    "i_rec_end_date" -> new ColumnStats(3L, 150000L, 12.0D, 12, null, null),
    "i_manufact_id" -> new ColumnStats(996L, 760L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_manager_id" -> new ColumnStats(100L, 742L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_container" -> new ColumnStats(2L, 0L, 6.982593333333333D, 7, null, null),
    "i_formulation" -> new ColumnStats(225244L, 0L, 19.947066666666668D, 20, null, null),
    "i_product_name" -> new ColumnStats(299277L, 0L, 22.83397666666667D, 30, null, null),
    "i_item_desc" -> new ColumnStats(219604L, 0L, 100.38903D, 200, null, null),
    "i_category_id" -> new ColumnStats(10L, 762L, 8.0D, 8, convertToNumber("10",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_item_id" -> new ColumnStats(151249L, 0L, 16.0D, 16, null, null),
    "i_class" -> new ColumnStats(99L, 0L, 7.76246D, 15, null, null),
    "i_current_price" -> new ColumnStats(9638L, 727L, 12.0D, 12, convertToNumber("99.99",DataTypes.DECIMAL(7, 2)), convertToNumber("0.09",DataTypes.DECIMAL(7, 2))),
    "i_category" -> new ColumnStats(11L, 0L, 5.88197D, 11, null, null),
    "i_brand_id" -> new ColumnStats(953L, 758L, 8.0D, 8, convertToNumber("10016017",DataTypes.BIGINT), convertToNumber("1001001",DataTypes.BIGINT)),
    "i_item_sk" -> new ColumnStats(298759L, 0L, 8.0D, 8, convertToNumber("300000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_manufact" -> new ColumnStats(999L, 0L, 11.282623333333333D, 15, null, null),
    "i_size" -> new ColumnStats(8L, 0L, 4.32403D, 11, null, null),
    "i_color" -> new ColumnStats(93L, 0L, 5.3702733333333335D, 10, null, null),
    "i_class_id" -> new ColumnStats(16L, 741L, 8.0D, 8, convertToNumber("16",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_wholesale_cost" -> new ColumnStats(7198L, 732L, 12.0D, 12, convertToNumber("89.48",DataTypes.DECIMAL(7, 2)), convertToNumber("0.02",DataTypes.DECIMAL(7, 2)))))
  val promotion = new TableStats(1500L, Map[String, ColumnStats](
    "p_channel_radio" -> new ColumnStats(2L, 0L, 0.9866666666666667D, 1, null, null),
    "p_item_sk" -> new ColumnStats(1481L, 12L, 8.0D, 8, convertToNumber("299786",DataTypes.BIGINT), convertToNumber("184",DataTypes.BIGINT)),
    "p_channel_catalog" -> new ColumnStats(2L, 0L, 0.9893333333333333D, 1, null, null),
    "p_response_target" -> new ColumnStats(1L, 17L, 8.0D, 8, convertToNumber("1",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "p_start_date_sk" -> new ColumnStats(679L, 12L, 8.0D, 8, convertToNumber("2450915",DataTypes.BIGINT), convertToNumber("2450095",DataTypes.BIGINT)),
    "p_discount_active" -> new ColumnStats(2L, 0L, 0.9953333333333333D, 1, null, null),
    "p_promo_id" -> new ColumnStats(1494L, 0L, 16.0D, 16, null, null),
    "p_promo_name" -> new ColumnStats(11L, 0L, 3.9506666666666668D, 5, null, null),
    "p_cost" -> new ColumnStats(1L, 15L, 12.0D, 12, convertToNumber("1000.0",DataTypes.DECIMAL(15, 2)), convertToNumber("1000.0",DataTypes.DECIMAL(15, 2))),
    "p_purpose" -> new ColumnStats(2L, 0L, 6.897333333333333D, 7, null, null),
    "p_channel_dmail" -> new ColumnStats(3L, 0L, 0.992D, 1, null, null),
    "p_channel_press" -> new ColumnStats(2L, 0L, 0.9893333333333333D, 1, null, null),
    "p_end_date_sk" -> new ColumnStats(710L, 9L, 8.0D, 8, convertToNumber("2450962",DataTypes.BIGINT), convertToNumber("2450098",DataTypes.BIGINT)),
    "p_channel_email" -> new ColumnStats(2L, 0L, 0.9913333333333333D, 1, null, null),
    "p_channel_tv" -> new ColumnStats(2L, 0L, 0.9893333333333333D, 1, null, null),
    "p_promo_sk" -> new ColumnStats(1507L, 0L, 8.0D, 8, convertToNumber("1500",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "p_channel_event" -> new ColumnStats(2L, 0L, 0.9893333333333333D, 1, null, null),
    "p_channel_demo" -> new ColumnStats(2L, 0L, 0.9906666666666667D, 1, null, null),
    "p_channel_details" -> new ColumnStats(1491L, 0L, 39.70066666666666D, 60, null, null)))
  val reason = new TableStats(65L, Map[String, ColumnStats](
    "r_reason_sk" -> new ColumnStats(65L, 0L, 8.0D, 8, convertToNumber("65",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "r_reason_id" -> new ColumnStats(65L, 0L, 16.0D, 16, null, null),
    "r_reason_desc" -> new ColumnStats(64L, 0L, 13.046153846153846D, 43, null, null)))
  val ship_mode = new TableStats(20L, Map[String, ColumnStats](
    "sm_type" -> new ColumnStats(6L, 0L, 7.5D, 9, null, null),
    "sm_ship_mode_id" -> new ColumnStats(20L, 0L, 16.0D, 16, null, null),
    "sm_ship_mode_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sm_contract" -> new ColumnStats(20L, 0L, 11.4D, 20, null, null),
    "sm_code" -> new ColumnStats(4L, 0L, 4.35D, 7, null, null),
    "sm_carrier" -> new ColumnStats(20L, 0L, 6.65D, 14, null, null)))
  val store = new TableStats(1002L, Map[String, ColumnStats](
    "s_country" -> new ColumnStats(2L, 0L, 12.922155688622755D, 13, null, null),
    "s_tax_precentage" -> new ColumnStats(12L, 5L, 12.0D, 12, convertToNumber("0.11",DataTypes.DECIMAL(5, 2)), convertToNumber("0.0",DataTypes.DECIMAL(5, 2))),
    "s_market_desc" -> new ColumnStats(742L, 0L, 57.66067864271457D, 100, null, null),
    "s_store_sk" -> new ColumnStats(998L, 0L, 8.0D, 8, convertToNumber("1002",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "s_city" -> new ColumnStats(55L, 0L, 9.148702594810379D, 15, null, null),
    "s_store_id" -> new ColumnStats(501L, 0L, 16.0D, 16, null, null),
    "s_suite_number" -> new ColumnStats(76L, 0L, 7.849301397205589D, 9, null, null),
    "s_company_id" -> new ColumnStats(1L, 4L, 8.0D, 8, convertToNumber("1",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "s_store_name" -> new ColumnStats(11L, 0L, 3.936127744510978D, 5, null, null),
    "s_floor_space" -> new ColumnStats(744L, 5L, 8.0D, 8, convertToNumber("9998983",DataTypes.BIGINT), convertToNumber("5004035",DataTypes.BIGINT)),
    "s_street_number" -> new ColumnStats(538L, 0L, 2.873253493013972D, 4, null, null),
    "s_street_type" -> new ColumnStats(21L, 0L, 4.11377245508982D, 9, null, null),
    "s_number_employees" -> new ColumnStats(101L, 6L, 8.0D, 8, convertToNumber("300",DataTypes.BIGINT), convertToNumber("200",DataTypes.BIGINT)),
    "s_company_name" -> new ColumnStats(2L, 0L, 6.958083832335329D, 7, null, null),
    "s_division_name" -> new ColumnStats(2L, 0L, 6.958083832335329D, 7, null, null),
    "s_zip" -> new ColumnStats(337L, 0L, 4.970059880239521D, 5, null, null),
    "s_hours" -> new ColumnStats(4L, 0L, 7.1017964071856285D, 8, null, null),
    "s_manager" -> new ColumnStats(740L, 0L, 12.743512974051896D, 20, null, null),
    "s_market_manager" -> new ColumnStats(753L, 0L, 12.774451097804391D, 23, null, null),
    "s_geography_class" -> new ColumnStats(2L, 0L, 6.986027944111776D, 7, null, null),
    "s_gmt_offset" -> new ColumnStats(4L, 4L, 12.0D, 12, convertToNumber("-5.0",DataTypes.DECIMAL(5, 2)), convertToNumber("-8.0",DataTypes.DECIMAL(5, 2))),
    "s_state" -> new ColumnStats(22L, 0L, 1.9880239520958083D, 2, null, null),
    "s_street_name" -> new ColumnStats(551L, 0L, 8.547904191616766D, 18, null, null),
    "s_closed_date_sk" -> new ColumnStats(169L, 741L, 8.0D, 8, convertToNumber("2451309",DataTypes.BIGINT), convertToNumber("2450824",DataTypes.BIGINT)),
    "s_rec_start_date" -> new ColumnStats(4L, 7L, 12.0D, 12, null, null),
    "s_county" -> new ColumnStats(28L, 0L, 14.158682634730539D, 22, null, null),
    "s_division_id" -> new ColumnStats(1L, 7L, 8.0D, 8, convertToNumber("1",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "s_rec_end_date" -> new ColumnStats(3L, 501L, 12.0D, 12, null, null),
    "s_market_id" -> new ColumnStats(10L, 2L, 8.0D, 8, convertToNumber("10",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))
  val time_dim = new TableStats(86400L, Map[String, ColumnStats](
    "t_minute" -> new ColumnStats(60L, 0L, 8.0D, 8, convertToNumber("59",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "t_am_pm" -> new ColumnStats(2L, 0L, 2.0D, 2, null, null),
    "t_time_sk" -> new ColumnStats(86180L, 0L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "t_time" -> new ColumnStats(86180L, 0L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "t_time_id" -> new ColumnStats(86824L, 0L, 16.0D, 16, null, null),
    "t_second" -> new ColumnStats(60L, 0L, 8.0D, 8, convertToNumber("59",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "t_meal_time" -> new ColumnStats(4L, 0L, 2.875D, 9, null, null),
    "t_shift" -> new ColumnStats(3L, 0L, 5.333333333333333D, 6, null, null),
    "t_hour" -> new ColumnStats(24L, 0L, 8.0D, 8, convertToNumber("23",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "t_sub_shift" -> new ColumnStats(4L, 0L, 6.916666666666667D, 9, null, null)))
  val warehouse = new TableStats(20L, Map[String, ColumnStats](
    "w_state" -> new ColumnStats(10L, 0L, 2.0D, 2, null, null),
    "w_gmt_offset" -> new ColumnStats(2L, 0L, 12.0D, 12, convertToNumber("-5.0",DataTypes.DECIMAL(5, 2)), convertToNumber("-6.0",DataTypes.DECIMAL(5, 2))),
    "w_warehouse_id" -> new ColumnStats(20L, 0L, 16.0D, 16, null, null),
    "w_county" -> new ColumnStats(11L, 0L, 14.75D, 17, null, null),
    "w_zip" -> new ColumnStats(18L, 0L, 5.0D, 5, null, null),
    "w_city" -> new ColumnStats(14L, 0L, 8.9D, 14, null, null),
    "w_country" -> new ColumnStats(1L, 0L, 13.0D, 13, null, null),
    "w_warehouse_name" -> new ColumnStats(20L, 0L, 15.1D, 19, null, null),
    "w_street_type" -> new ColumnStats(13L, 0L, 3.8D, 7, null, null),
    "w_street_name" -> new ColumnStats(19L, 0L, 8.7D, 16, null, null),
    "w_street_number" -> new ColumnStats(20L, 0L, 3.0D, 3, null, null),
    "w_warehouse_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "w_warehouse_sq_ft" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("984998",DataTypes.BIGINT), convertToNumber("51874",DataTypes.BIGINT)),
    "w_suite_number" -> new ColumnStats(19L, 0L, 8.2D, 9, null, null)))
  val web_page = new TableStats(3000L, Map[String, ColumnStats](
    "wp_image_count" -> new ColumnStats(7L, 32L, 8.0D, 8, convertToNumber("7",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wp_char_count" -> new ColumnStats(1875L, 40L, 8.0D, 8, convertToNumber("8157",DataTypes.BIGINT), convertToNumber("306",DataTypes.BIGINT)),
    "wp_autogen_flag" -> new ColumnStats(3L, 0L, 0.9866666666666667D, 1, null, null),
    "wp_creation_date_sk" -> new ColumnStats(195L, 40L, 8.0D, 8, convertToNumber("2450815",DataTypes.BIGINT), convertToNumber("2450607",DataTypes.BIGINT)),
    "wp_link_count" -> new ColumnStats(24L, 38L, 8.0D, 8, convertToNumber("25",DataTypes.BIGINT), convertToNumber("2",DataTypes.BIGINT)),
    "wp_rec_start_date" -> new ColumnStats(4L, 38L, 12.0D, 12, null, null),
    "wp_web_page_id" -> new ColumnStats(1512L, 0L, 16.0D, 16, null, null),
    "wp_rec_end_date" -> new ColumnStats(3L, 1500L, 12.0D, 12, null, null),
    "wp_access_date_sk" -> new ColumnStats(100L, 41L, 8.0D, 8, convertToNumber("2452648",DataTypes.BIGINT), convertToNumber("2452548",DataTypes.BIGINT)),
    "wp_customer_sk" -> new ColumnStats(750L, 2108L, 8.0D, 8, convertToNumber("11988869",DataTypes.BIGINT), convertToNumber("2379",DataTypes.BIGINT)),
    "wp_web_page_sk" -> new ColumnStats(2992L, 0L, 8.0D, 8, convertToNumber("3000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wp_max_ad_count" -> new ColumnStats(5L, 36L, 8.0D, 8, convertToNumber("4",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "wp_type" -> new ColumnStats(8L, 0L, 6.372333333333334D, 9, null, null),
    "wp_url" -> new ColumnStats(2L, 0L, 17.754D, 18, null, null)))
  val web_site = new TableStats(54L, Map[String, ColumnStats](
    "web_market_manager" -> new ColumnStats(40L, 0L, 12.148148148148149D, 17, null, null),
    "web_country" -> new ColumnStats(2L, 0L, 12.75925925925926D, 13, null, null),
    "web_open_date_sk" -> new ColumnStats(27L, 0L, 8.0D, 8, convertToNumber("2450807",DataTypes.BIGINT), convertToNumber("2450373",DataTypes.BIGINT)),
    "web_street_type" -> new ColumnStats(20L, 0L, 3.759259259259259D, 9, null, null),
    "web_zip" -> new ColumnStats(39L, 0L, 4.814814814814815D, 5, null, null),
    "web_gmt_offset" -> new ColumnStats(3L, 0L, 12.0D, 12, convertToNumber("-5.0",DataTypes.DECIMAL(5, 2)), convertToNumber("-7.0",DataTypes.DECIMAL(5, 2))),
    "web_street_number" -> new ColumnStats(40L, 0L, 2.925925925925926D, 3, null, null),
    "web_state" -> new ColumnStats(20L, 0L, 1.962962962962963D, 2, null, null),
    "web_suite_number" -> new ColumnStats(40L, 0L, 8.092592592592593D, 9, null, null),
    "web_rec_end_date" -> new ColumnStats(3L, 27L, 12.0D, 12, null, null),
    "web_close_date_sk" -> new ColumnStats(18L, 10L, 8.0D, 8, convertToNumber("2446218",DataTypes.BIGINT), convertToNumber("2441265",DataTypes.BIGINT)),
    "web_company_name" -> new ColumnStats(6L, 0L, 3.925925925925926D, 5, null, null),
    "web_manager" -> new ColumnStats(40L, 0L, 12.75925925925926D, 20, null, null),
    "web_mkt_class" -> new ColumnStats(38L, 0L, 34.888888888888886D, 50, null, null),
    "web_mkt_id" -> new ColumnStats(5L, 1L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "web_name" -> new ColumnStats(10L, 0L, 5.777777777777778D, 6, null, null),
    "web_tax_percentage" -> new ColumnStats(12L, 2L, 12.0D, 12, convertToNumber("0.11",DataTypes.DECIMAL(5, 2)), convertToNumber("0.0",DataTypes.DECIMAL(5, 2))),
    "web_company_id" -> new ColumnStats(6L, 1L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "web_site_sk" -> new ColumnStats(54L, 0L, 8.0D, 8, convertToNumber("54",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "web_mkt_desc" -> new ColumnStats(39L, 0L, 59.611111111111114D, 93, null, null),
    "web_rec_start_date" -> new ColumnStats(4L, 0L, 12.0D, 12, null, null),
    "web_class" -> new ColumnStats(2L, 0L, 6.87037037037037D, 7, null, null),
    "web_city" -> new ColumnStats(33L, 0L, 9.24074074074074D, 14, null, null),
    "web_county" -> new ColumnStats(25L, 0L, 13.62962962962963D, 22, null, null),
    "web_site_id" -> new ColumnStats(27L, 0L, 16.0D, 16, null, null),
    "web_street_name" -> new ColumnStats(48L, 0L, 8.296296296296296D, 15, null, null)))

  def loadStats(table: String): TableStats = {
    table match {
      case "catalog_sales" => catalog_sales
      case "catalog_returns" => catalog_returns
      case "inventory" => inventory
      case "store_sales" => store_sales
      case "store_returns" => store_returns
      case "web_sales" => web_sales
      case "web_returns" => web_returns
      case "call_center" => call_center
      case "catalog_page" => catalog_page
      case "customer" => customer
      case "customer_address" => customer_address
      case "customer_demographics" => customer_demographics
      case "date_dim" => date_dim
      case "household_demographics" => household_demographics
      case "income_band" => income_band
      case "item" => item
      case "promotion" => promotion
      case "reason" => reason
      case "ship_mode" => ship_mode
      case "store" => store
      case "time_dim" => time_dim
      case "warehouse" => warehouse
      case "web_page" => web_page
      case "web_site" => web_site
      case _ => null
    }
  }

  private def convertToNumber(value: String, dataType: DataType): Number = dataType.getLogicalType.getTypeRoot match {
    case TINYINT =>
      java.lang.Byte.valueOf(value)
    case SMALLINT =>
      java.lang.Short.valueOf(value)
    case INTEGER =>
      java.lang.Integer.valueOf(value)
    case BIGINT =>
      java.lang.Long.valueOf(value)
    case FLOAT =>
      java.lang.Float.valueOf(value)
    case DOUBLE =>
      java.lang.Double.valueOf(value)
    case DECIMAL =>
      java.math.BigDecimal.valueOf(java.lang.Double.valueOf(value))
    case _ =>
      null
  }
}


