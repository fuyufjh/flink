package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._

import scala.collection.JavaConversions._

object PredefinedStatistics {

  val CATALOG_SALES_100 = new TableStats(144004615L, Map[String, ColumnStats](
    "cs_ship_mode_sk" -> new ColumnStats(20L, 720600L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_bill_customer_sk" -> new ColumnStats(1994594L, 720189L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_sales_price" -> new ColumnStats(29584L, 720499L, 12.0D, 12, convertToNumber("300.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_net_profit" -> new ColumnStats(1718721L, 0L, 12.0D, 12, convertToNumber("19738.00",DataTypes.DECIMAL(7, 2)), convertToNumber("-10000.00",DataTypes.DECIMAL(7, 2))),
    "cs_ext_tax" -> new ColumnStats(189370L, 720724L, 12.0D, 12, convertToNumber("2648.76",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_wholesale_cost" -> new ColumnStats(9872L, 720655L, 12.0D, 12, convertToNumber("100.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "cs_ext_wholesale_cost" -> new ColumnStats(391162L, 720707L, 12.0D, 12, convertToNumber("10000.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "cs_ship_hdemo_sk" -> new ColumnStats(7207L, 721115L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_bill_addr_sk" -> new ColumnStats(996123L, 720628L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid_inc_ship_tax" -> new ColumnStats(2706015L, 0L, 12.0D, 12, convertToNumber("46004.19",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_ship_customer_sk" -> new ColumnStats(1994132L, 720047L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_ext_discount_amt" -> new ColumnStats(1022890L, 719424L, 12.0D, 12, convertToNumber("29682.18",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_sold_date_sk" -> new ColumnStats(1840L, 720219L, 8.0D, 8, convertToNumber("2452654",DataTypes.BIGINT), convertToNumber("2450815",DataTypes.BIGINT)),
    "cs_order_number" -> new ColumnStats(15987217L, 0L, 8.0D, 8, convertToNumber("16000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_quantity" -> new ColumnStats(100L, 721032L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_bill_cdemo_sk" -> new ColumnStats(1913252L, 721175L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_call_center_sk" -> new ColumnStats(30L, 721589L, 8.0D, 8, convertToNumber("30",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid_inc_tax" -> new ColumnStats(2021672L, 721187L, 12.0D, 12, convertToNumber("32079.48",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_catalog_page_sk" -> new ColumnStats(11343L, 720974L, 8.0D, 8, convertToNumber("17108",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_ship_cdemo_sk" -> new ColumnStats(1913068L, 721518L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_warehouse_sk" -> new ColumnStats(15L, 721553L, 8.0D, 8, convertToNumber("15",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid" -> new ColumnStats(1439257L, 720505L, 12.0D, 12, convertToNumber("29707.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_ext_list_price" -> new ColumnStats(1162880L, 720226L, 12.0D, 12, convertToNumber("29997.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "cs_list_price" -> new ColumnStats(29854L, 720126L, 12.0D, 12, convertToNumber("300.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "cs_promo_sk" -> new ColumnStats(996L, 719991L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_ship_addr_sk" -> new ColumnStats(996123L, 721126L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_net_paid_inc_ship" -> new ColumnStats(1993520L, 0L, 12.0D, 12, convertToNumber("44263.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_ext_sales_price" -> new ColumnStats(1024632L, 720925L, 12.0D, 12, convertToNumber("29707.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_ship_date_sk" -> new ColumnStats(1932L, 720368L, 8.0D, 8, convertToNumber("2452744",DataTypes.BIGINT), convertToNumber("2450817",DataTypes.BIGINT)),
    "cs_ext_ship_cost" -> new ColumnStats(536667L, 720698L, 12.0D, 12, convertToNumber("14905.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_coupon_amt" -> new ColumnStats(1095532L, 720409L, 12.0D, 12, convertToNumber("28824.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cs_bill_hdemo_sk" -> new ColumnStats(7207L, 721108L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cs_sold_time_sk" -> new ColumnStats(86180L, 721080L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT))))


  val CATALOG_RETURNS_100 = new TableStats(14400181L, Map[String, ColumnStats](
    "cr_order_number" -> new ColumnStats(9543234L, 0L, 8.0D, 8, convertToNumber("15999996",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_return_amount" -> new ColumnStats(614594L, 288032L, 12.0D, 12, convertToNumber("28052.64",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_refunded_addr_sk" -> new ColumnStats(996123L, 288642L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_net_loss" -> new ColumnStats(591027L, 288261L, 12.0D, 12, convertToNumber("16390.83",DataTypes.DECIMAL(7, 2)), convertToNumber("0.50",DataTypes.DECIMAL(7, 2))),
    "cr_return_amt_inc_tax" -> new ColumnStats(928752L, 287469L, 12.0D, 12, convertToNumber("30292.01",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_catalog_page_sk" -> new ColumnStats(11343L, 287952L, 8.0D, 8, convertToNumber("17108",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returning_customer_sk" -> new ColumnStats(1993486L, 287880L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_store_credit" -> new ColumnStats(436486L, 288145L, 12.0D, 12, convertToNumber("22796.77",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_returning_addr_sk" -> new ColumnStats(996123L, 288329L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returning_hdemo_sk" -> new ColumnStats(7207L, 287536L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_fee" -> new ColumnStats(9919L, 287772L, 12.0D, 12, convertToNumber("100.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.50",DataTypes.DECIMAL(7, 2))),
    "cr_refunded_cash" -> new ColumnStats(631613L, 288035L, 12.0D, 12, convertToNumber("26390.01",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_return_ship_cost" -> new ColumnStats(352071L, 288100L, 12.0D, 12, convertToNumber("14406.98",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_returned_time_sk" -> new ColumnStats(86180L, 0L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cr_return_tax" -> new ColumnStats(106432L, 288302L, 12.0D, 12, convertToNumber("2501.17",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_refunded_customer_sk" -> new ColumnStats(1974737L, 288019L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_ship_mode_sk" -> new ColumnStats(20L, 288644L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_refunded_cdemo_sk" -> new ColumnStats(1900656L, 288171L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returned_date_sk" -> new ColumnStats(2108L, 0L, 8.0D, 8, convertToNumber("2452923",DataTypes.BIGINT), convertToNumber("2450823",DataTypes.BIGINT)),
    "cr_reversed_charge" -> new ColumnStats(442140L, 288023L, 12.0D, 12, convertToNumber("22692.77",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "cr_call_center_sk" -> new ColumnStats(30L, 288270L, 8.0D, 8, convertToNumber("30",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_refunded_hdemo_sk" -> new ColumnStats(7207L, 288119L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_warehouse_sk" -> new ColumnStats(15L, 288089L, 8.0D, 8, convertToNumber("15",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_returning_cdemo_sk" -> new ColumnStats(1912542L, 288065L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_return_quantity" -> new ColumnStats(100L, 288179L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cr_reason_sk" -> new ColumnStats(55L, 288042L, 8.0D, 8, convertToNumber("55",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))

  val INVENTORY_100 = new TableStats(399330000L, Map[String, ColumnStats](
    "inv_date_sk" -> new ColumnStats(261L, 0L, 8.0D, 8, convertToNumber("2452635",DataTypes.BIGINT), convertToNumber("2450815",DataTypes.BIGINT)),
    "inv_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "inv_warehouse_sk" -> new ColumnStats(15L, 0L, 8.0D, 8, convertToNumber("15",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "inv_quantity_on_hand" -> new ColumnStats(1003L, 19960731L, 4.0D, 4, convertToNumber("1000",DataTypes.INT), convertToNumber("0",DataTypes.INT))))

  val STORE_SALES_100 = new TableStats(287999114L, Map[String, ColumnStats](
    "ss_quantity" -> new ColumnStats(100L, 12951419L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_wholesale_cost" -> new ColumnStats(9872L, 12957974L, 12.0D, 12, convertToNumber("100.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ss_net_paid" -> new ColumnStats(1134095L, 12952987L, 12.0D, 12, convertToNumber("19964.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_net_profit" -> new ColumnStats(1395333L, 12955481L, 12.0D, 12, convertToNumber("9982.00",DataTypes.DECIMAL(7, 2)), convertToNumber("-10000.00",DataTypes.DECIMAL(7, 2))),
    "ss_net_paid_inc_tax" -> new ColumnStats(1544710L, 12960167L, 12.0D, 12, convertToNumber("21673.56",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_sales_price" -> new ColumnStats(19717L, 12956087L, 12.0D, 12, convertToNumber("200.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_ticket_number" -> new ColumnStats(23765095L, 0L, 8.0D, 8, convertToNumber("24000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_ext_wholesale_cost" -> new ColumnStats(391162L, 12955797L, 12.0D, 12, convertToNumber("10000.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ss_cdemo_sk" -> new ColumnStats(1913326L, 12955277L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_promo_sk" -> new ColumnStats(996L, 12954990L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_sold_date_sk" -> new ColumnStats(1826L, 12952486L, 8.0D, 8, convertToNumber("2452642",DataTypes.BIGINT), convertToNumber("2450816",DataTypes.BIGINT)),
    "ss_ext_sales_price" -> new ColumnStats(733630L, 12951512L, 12.0D, 12, convertToNumber("19964.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_sold_time_sk" -> new ColumnStats(46629L, 12949839L, 8.0D, 8, convertToNumber("75599",DataTypes.BIGINT), convertToNumber("28800",DataTypes.BIGINT)),
    "ss_customer_sk" -> new ColumnStats(1994594L, 12952782L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_list_price" -> new ColumnStats(19607L, 12954734L, 12.0D, 12, convertToNumber("200.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ss_store_sk" -> new ColumnStats(201L, 12950286L, 8.0D, 8, convertToNumber("400",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_addr_sk" -> new ColumnStats(996123L, 12954974L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_ext_discount_amt" -> new ColumnStats(966329L, 12957946L, 12.0D, 12, convertToNumber("18874.26",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_coupon_amt" -> new ColumnStats(966329L, 12957946L, 12.0D, 12, convertToNumber("18874.26",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_ext_tax" -> new ColumnStats(141522L, 12955333L, 12.0D, 12, convertToNumber("1772.01",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ss_hdemo_sk" -> new ColumnStats(7207L, 12958386L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ss_ext_list_price" -> new ColumnStats(777139L, 12954967L, 12.0D, 12, convertToNumber("20000.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2)))))

  val STORE_RETURNS_100 = new TableStats(28801297L, Map[String, ColumnStats](
    "sr_return_time_sk" -> new ColumnStats(32528L, 1007923L, 8.0D, 8, convertToNumber("61199",DataTypes.BIGINT), convertToNumber("28799",DataTypes.BIGINT)),
    "sr_return_ship_cost" -> new ColumnStats(291713L, 1009160L, 12.0D, 12, convertToNumber("9350.80",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "sr_cdemo_sk" -> new ColumnStats(1913326L, 1007357L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_store_credit" -> new ColumnStats(426189L, 1007228L, 12.0D, 12, convertToNumber("16377.94",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "sr_return_amt_inc_tax" -> new ColumnStats(865496L, 1008069L, 12.0D, 12, convertToNumber("20239.89",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "sr_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_return_quantity" -> new ColumnStats(100L, 1007981L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_net_loss" -> new ColumnStats(524615L, 1007350L, 12.0D, 12, convertToNumber("10257.03",DataTypes.DECIMAL(7, 2)), convertToNumber("0.50",DataTypes.DECIMAL(7, 2))),
    "sr_ticket_number" -> new ColumnStats(16903316L, 0L, 8.0D, 8, convertToNumber("24000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_refunded_cash" -> new ColumnStats(597419L, 1007729L, 12.0D, 12, convertToNumber("18599.42",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "sr_hdemo_sk" -> new ColumnStats(7207L, 1008032L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_fee" -> new ColumnStats(9919L, 1008866L, 12.0D, 12, convertToNumber("100.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.50",DataTypes.DECIMAL(7, 2))),
    "sr_addr_sk" -> new ColumnStats(996123L, 1007642L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_returned_date_sk" -> new ColumnStats(2007L, 1008333L, 8.0D, 8, convertToNumber("2452822",DataTypes.BIGINT), convertToNumber("2450820",DataTypes.BIGINT)),
    "sr_store_sk" -> new ColumnStats(201L, 1008720L, 8.0D, 8, convertToNumber("400",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_return_tax" -> new ColumnStats(89572L, 1008205L, 12.0D, 12, convertToNumber("1671.18",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "sr_customer_sk" -> new ColumnStats(1994594L, 1009014L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_reason_sk" -> new ColumnStats(55L, 1007879L, 8.0D, 8, convertToNumber("55",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sr_reversed_charge" -> new ColumnStats(430479L, 1009091L, 12.0D, 12, convertToNumber("15525.86",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "sr_return_amt" -> new ColumnStats(527454L, 1007037L, 12.0D, 12, convertToNumber("19687.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2)))))

  val WEB_SALES_100 = new TableStats(72006020L, Map[String, ColumnStats](
    "ws_ship_mode_sk" -> new ColumnStats(20L, 18205L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_paid_inc_ship" -> new ColumnStats(1783862L, 0L, 12.0D, 12, convertToNumber("43956.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_coupon_amt" -> new ColumnStats(914603L, 18113L, 12.0D, 12, convertToNumber("28422.94",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_ext_discount_amt" -> new ColumnStats(962079L, 18171L, 12.0D, 12, convertToNumber("29765.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_order_number" -> new ColumnStats(5951214L, 0L, 8.0D, 8, convertToNumber("6000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ext_list_price" -> new ColumnStats(1143720L, 18111L, 12.0D, 12, convertToNumber("29997.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ws_bill_customer_sk" -> new ColumnStats(1896770L, 18015L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_promo_sk" -> new ColumnStats(996L, 18101L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_profit" -> new ColumnStats(1564925L, 0L, 12.0D, 12, convertToNumber("19752.00",DataTypes.DECIMAL(7, 2)), convertToNumber("-9999.00",DataTypes.DECIMAL(7, 2))),
    "ws_net_paid" -> new ColumnStats(1305905L, 17973L, 12.0D, 12, convertToNumber("29728.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_paid_inc_ship_tax" -> new ColumnStats(2435670L, 0L, 12.0D, 12, convertToNumber("46593.36",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_web_page_sk" -> new ColumnStats(2051L, 17905L, 8.0D, 8, convertToNumber("2040",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_warehouse_sk" -> new ColumnStats(15L, 17821L, 8.0D, 8, convertToNumber("15",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_cdemo_sk" -> new ColumnStats(1827172L, 17945L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_sold_date_sk" -> new ColumnStats(1826L, 18020L, 8.0D, 8, convertToNumber("2452642",DataTypes.BIGINT), convertToNumber("2450816",DataTypes.BIGINT)),
    "ws_list_price" -> new ColumnStats(29854L, 18007L, 12.0D, 12, convertToNumber("300.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ws_sold_time_sk" -> new ColumnStats(86180L, 17866L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "ws_ext_ship_cost" -> new ColumnStats(514879L, 18022L, 12.0D, 12, convertToNumber("14896.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_quantity" -> new ColumnStats(100L, 18102L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_hdemo_sk" -> new ColumnStats(7207L, 18239L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ext_tax" -> new ColumnStats(176337L, 18027L, 12.0D, 12, convertToNumber("2637.36",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_sales_price" -> new ColumnStats(29360L, 17964L, 12.0D, 12, convertToNumber("300.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_ext_wholesale_cost" -> new ColumnStats(391162L, 17904L, 12.0D, 12, convertToNumber("10000.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ws_bill_cdemo_sk" -> new ColumnStats(1828698L, 18194L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_net_paid_inc_tax" -> new ColumnStats(1807579L, 18241L, 12.0D, 12, convertToNumber("31941.36",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_ship_addr_sk" -> new ColumnStats(992389L, 17992L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_bill_addr_sk" -> new ColumnStats(992946L, 18041L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_date_sk" -> new ColumnStats(1952L, 18133L, 8.0D, 8, convertToNumber("2452762",DataTypes.BIGINT), convertToNumber("2450817",DataTypes.BIGINT)),
    "ws_bill_hdemo_sk" -> new ColumnStats(7207L, 17937L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ship_customer_sk" -> new ColumnStats(1894323L, 18105L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ws_ext_sales_price" -> new ColumnStats(969350L, 17879L, 12.0D, 12, convertToNumber("29728.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "ws_wholesale_cost" -> new ColumnStats(9872L, 17921L, 12.0D, 12, convertToNumber("100.00",DataTypes.DECIMAL(7, 2)), convertToNumber("1.00",DataTypes.DECIMAL(7, 2))),
    "ws_web_site_sk" -> new ColumnStats(24L, 18133L, 8.0D, 8, convertToNumber("24",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))

  val WEB_RETURNS_100 = new TableStats(7201591L, Map[String, ColumnStats](
    "wr_reason_sk" -> new ColumnStats(55L, 324077L, 8.0D, 8, convertToNumber("55",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_fee" -> new ColumnStats(9919L, 324630L, 12.0D, 12, convertToNumber("100.00",DataTypes.DECIMAL(7, 2)), convertToNumber("0.50",DataTypes.DECIMAL(7, 2))),
    "wr_return_amt" -> new ColumnStats(508211L, 323591L, 12.0D, 12, convertToNumber("28066.80",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_returning_addr_sk" -> new ColumnStats(994988L, 324281L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_returned_time_sk" -> new ColumnStats(86180L, 323011L, 8.0D, 8, convertToNumber("86399",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "wr_returning_cdemo_sk" -> new ColumnStats(1857735L, 323802L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_web_page_sk" -> new ColumnStats(2051L, 323204L, 8.0D, 8, convertToNumber("2040",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_tax" -> new ColumnStats(91649L, 323424L, 12.0D, 12, convertToNumber("2188.12",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_refunded_customer_sk" -> new ColumnStats(1937956L, 324483L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_ship_cost" -> new ColumnStats(303361L, 323884L, 12.0D, 12, convertToNumber("13424.80",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_refunded_hdemo_sk" -> new ColumnStats(7207L, 323813L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_account_credit" -> new ColumnStats(347992L, 324017L, 12.0D, 12, convertToNumber("24291.90",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_returned_date_sk" -> new ColumnStats(2188L, 324020L, 8.0D, 8, convertToNumber("2453001",DataTypes.BIGINT), convertToNumber("2450823",DataTypes.BIGINT)),
    "wr_reversed_charge" -> new ColumnStats(348917L, 323773L, 12.0D, 12, convertToNumber("23538.14",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_order_number" -> new ColumnStats(4222047L, 0L, 8.0D, 8, convertToNumber("5999999",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_returning_customer_sk" -> new ColumnStats(1934130L, 323567L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_amt_inc_tax" -> new ColumnStats(739168L, 324478L, 12.0D, 12, convertToNumber("29106.73",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_returning_hdemo_sk" -> new ColumnStats(7207L, 324920L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_return_quantity" -> new ColumnStats(100L, 324489L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_refunded_cash" -> new ColumnStats(503637L, 323391L, 12.0D, 12, convertToNumber("24927.43",DataTypes.DECIMAL(7, 2)), convertToNumber("0.00",DataTypes.DECIMAL(7, 2))),
    "wr_refunded_addr_sk" -> new ColumnStats(994913L, 323796L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_refunded_cdemo_sk" -> new ColumnStats(1860281L, 323992L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wr_net_loss" -> new ColumnStats(494378L, 323893L, 12.0D, 12, convertToNumber("14811.32",DataTypes.DECIMAL(7, 2)), convertToNumber("0.50",DataTypes.DECIMAL(7, 2)))))

  val CALL_CENTER_100 = new TableStats(30L, Map[String, ColumnStats](
    "cc_street_number" -> new ColumnStats(15L, 0L, 3.0D, 3, null, null),
    "cc_call_center_id" -> new ColumnStats(15L, 0L, 16.0D, 16, null, null),
    "cc_state" -> new ColumnStats(9L, 0L, 2.0D, 2, null, null),
    "cc_tax_percentage" -> new ColumnStats(11L, 0L, 12.0D, 12, convertToNumber("0.11",DataTypes.DECIMAL(5, 2)), convertToNumber("0.00",DataTypes.DECIMAL(5, 2))),
    "cc_division_name" -> new ColumnStats(6L, 0L, 3.7D, 5, null, null),
    "cc_hours" -> new ColumnStats(3L, 0L, 7.233333333333333D, 8, null, null),
    "cc_manager" -> new ColumnStats(23L, 0L, 13.133333333333333D, 17, null, null),
    "cc_name" -> new ColumnStats(15L, 0L, 13.366666666666667D, 19, null, null),
    "cc_employees" -> new ColumnStats(23L, 0L, 8.0D, 8, convertToNumber("64776",DataTypes.BIGINT), convertToNumber("730",DataTypes.BIGINT)),
    "cc_mkt_id" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cc_class" -> new ColumnStats(3L, 0L, 5.233333333333333D, 6, null, null),
    "cc_division" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cc_street_name" -> new ColumnStats(14L, 0L, 8.966666666666667D, 15, null, null),
    "cc_rec_end_date" -> new ColumnStats(3L, 15L, 12.0D, 12, null, null),
    "cc_gmt_offset" -> new ColumnStats(2L, 0L, 12.0D, 12, convertToNumber("-5.00",DataTypes.DECIMAL(5, 2)), convertToNumber("-6.00",DataTypes.DECIMAL(5, 2))),
    "cc_market_manager" -> new ColumnStats(21L, 0L, 11.933333333333334D, 14, null, null),
    "cc_open_date_sk" -> new ColumnStats(14L, 0L, 8.0D, 8, convertToNumber("2451104",DataTypes.BIGINT), convertToNumber("2450820",DataTypes.BIGINT)),
    "cc_country" -> new ColumnStats(1L, 0L, 13.0D, 13, null, null),
    "cc_closed_date_sk" -> new ColumnStats(0L, 30L, 8.0D, 8, null, null),
    "cc_company_name" -> new ColumnStats(6L, 0L, 3.966666666666667D, 5, null, null),
    "cc_city" -> new ColumnStats(12L, 0L, 8.9D, 14, null, null),
    "cc_company" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cc_county" -> new ColumnStats(9L, 0L, 14.3D, 17, null, null),
    "cc_rec_start_date" -> new ColumnStats(4L, 0L, 12.0D, 12, null, null),
    "cc_street_type" -> new ColumnStats(12L, 0L, 4.066666666666666D, 7, null, null),
    "cc_sq_ft" -> new ColumnStats(23L, 0L, 8.0D, 8, convertToNumber("32744448",DataTypes.BIGINT), convertToNumber("383980",DataTypes.BIGINT)),
    "cc_mkt_class" -> new ColumnStats(25L, 0L, 34.266666666666666D, 50, null, null),
    "cc_zip" -> new ColumnStats(14L, 0L, 5.0D, 5, null, null),
    "cc_mkt_desc" -> new ColumnStats(20L, 0L, 62.166666666666664D, 95, null, null),
    "cc_suite_number" -> new ColumnStats(15L, 0L, 7.933333333333334D, 9, null, null),
    "cc_call_center_sk" -> new ColumnStats(30L, 0L, 8.0D, 8, convertToNumber("30",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))

  val CATALOG_PAGE_100 = new TableStats(20400L, Map[String, ColumnStats](
    "cp_department" -> new ColumnStats(2L, 0L, 9.897549019607844D, 10, null, null),
    "cp_catalog_page_number" -> new ColumnStats(187L, 195L, 8.0D, 8, convertToNumber("188",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cp_catalog_page_sk" -> new ColumnStats(20293L, 0L, 8.0D, 8, convertToNumber("20400",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cp_end_date_sk" -> new ColumnStats(96L, 187L, 8.0D, 8, convertToNumber("2453186",DataTypes.BIGINT), convertToNumber("2450844",DataTypes.BIGINT)),
    "cp_catalog_page_id" -> new ColumnStats(20596L, 0L, 16.0D, 16, null, null),
    "cp_description" -> new ColumnStats(20149L, 0L, 73.8625D, 99, null, null),
    "cp_type" -> new ColumnStats(4L, 0L, 7.58936274509804D, 9, null, null),
    "cp_start_date_sk" -> new ColumnStats(91L, 208L, 8.0D, 8, convertToNumber("2453005",DataTypes.BIGINT), convertToNumber("2450815",DataTypes.BIGINT)),
    "cp_catalog_number" -> new ColumnStats(109L, 213L, 8.0D, 8, convertToNumber("109",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))

  val CUSTOMER_100 = new TableStats(2000000L, Map[String, ColumnStats](
    "c_email_address" -> new ColumnStats(1945504L, 0L, 26.499647D, 47, null, null),
    "c_birth_month" -> new ColumnStats(12L, 70283L, 8.0D, 8, convertToNumber("12",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_first_sales_date_sk" -> new ColumnStats(3623L, 70180L, 8.0D, 8, convertToNumber("2452648",DataTypes.BIGINT), convertToNumber("2448998",DataTypes.BIGINT)),
    "c_customer_id" -> new ColumnStats(2000553L, 0L, 16.0D, 16, null, null),
    "c_birth_country" -> new ColumnStats(208L, 0L, 8.3884965D, 20, null, null),
    "c_salutation" -> new ColumnStats(7L, 0L, 3.128295D, 4, null, null),
    "c_last_name" -> new ColumnStats(5005L, 0L, 5.9140605D, 13, null, null),
    "c_birth_day" -> new ColumnStats(31L, 70516L, 8.0D, 8, convertToNumber("31",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_current_cdemo_sk" -> new ColumnStats(1219014L, 70222L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_login" -> new ColumnStats(1L, 0L, 0.0D, 0, null, null),
    "c_last_review_date" -> new ColumnStats(365L, 70141L, 8.0D, 8, convertToNumber("2452648",DataTypes.BIGINT), convertToNumber("2452283",DataTypes.BIGINT)),
    "c_first_shipto_date_sk" -> new ColumnStats(3624L, 69904L, 8.0D, 8, convertToNumber("2452678",DataTypes.BIGINT), convertToNumber("2449028",DataTypes.BIGINT)),
    "c_current_addr_sk" -> new ColumnStats(864148L, 0L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("2",DataTypes.BIGINT)),
    "c_birth_year" -> new ColumnStats(69L, 70492L, 8.0D, 8, convertToNumber("1992",DataTypes.BIGINT), convertToNumber("1924",DataTypes.BIGINT)),
    "c_preferred_cust_flag" -> new ColumnStats(3L, 0L, 0.964927D, 1, null, null),
    "c_current_hdemo_sk" -> new ColumnStats(7207L, 70313L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_customer_sk" -> new ColumnStats(1994594L, 0L, 8.0D, 8, convertToNumber("2000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "c_first_name" -> new ColumnStats(5179L, 0L, 5.6322525D, 11, null, null)))

  val CUSTOMER_ADDRESS_100 = new TableStats(1000000L, Map[String, ColumnStats](
    "ca_state" -> new ColumnStats(52L, 0L, 1.94034D, 2, null, null),
    "ca_street_type" -> new ColumnStats(21L, 0L, 4.073323D, 9, null, null),
    "ca_gmt_offset" -> new ColumnStats(6L, 29953L, 12.0D, 12, convertToNumber("-5.00",DataTypes.DECIMAL(5, 2)), convertToNumber("-10.00",DataTypes.DECIMAL(5, 2))),
    "ca_location_type" -> new ColumnStats(4L, 0L, 8.731663D, 13, null, null),
    "ca_street_number" -> new ColumnStats(1005L, 0L, 2.806162D, 4, null, null),
    "ca_address_id" -> new ColumnStats(1008557L, 0L, 16.0D, 16, null, null),
    "ca_suite_number" -> new ColumnStats(76L, 0L, 7.651503D, 9, null, null),
    "ca_country" -> new ColumnStats(2L, 0L, 12.609662D, 13, null, null),
    "ca_zip" -> new ColumnStats(7811L, 0L, 4.85055D, 5, null, null),
    "ca_address_sk" -> new ColumnStats(996123L, 0L, 8.0D, 8, convertToNumber("1000000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ca_county" -> new ColumnStats(1856L, 0L, 13.541361D, 28, null, null),
    "ca_city" -> new ColumnStats(991L, 0L, 8.687439D, 20, null, null),
    "ca_street_name" -> new ColumnStats(8196L, 0L, 8.454865D, 21, null, null)))

  val CUSTOMER_DEMOGRAPHICS_100 = new TableStats(1920800L, Map[String, ColumnStats](
    "cd_demo_sk" -> new ColumnStats(1913326L, 0L, 8.0D, 8, convertToNumber("1920800",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "cd_education_status" -> new ColumnStats(7L, 0L, 9.571428571428571D, 15, null, null),
    "cd_credit_rating" -> new ColumnStats(4L, 0L, 7.0D, 9, null, null),
    "cd_purchase_estimate" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("10000",DataTypes.BIGINT), convertToNumber("500",DataTypes.BIGINT)),
    "cd_dep_college_count" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cd_dep_count" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cd_gender" -> new ColumnStats(2L, 0L, 1.0D, 1, null, null),
    "cd_dep_employed_count" -> new ColumnStats(7L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "cd_marital_status" -> new ColumnStats(5L, 0L, 1.0D, 1, null, null)))

  val DATE_DIM_100 = new TableStats(73049L, Map[String, ColumnStats](
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

  val HOUSEHOLD_DEMOGRAPHICS_100 = new TableStats(7200L, Map[String, ColumnStats](
    "hd_demo_sk" -> new ColumnStats(7207L, 0L, 8.0D, 8, convertToNumber("7200",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "hd_income_band_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "hd_dep_count" -> new ColumnStats(10L, 0L, 8.0D, 8, convertToNumber("9",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "hd_buy_potential" -> new ColumnStats(6L, 0L, 7.5D, 10, null, null),
    "hd_vehicle_count" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("4",DataTypes.BIGINT), convertToNumber("-1",DataTypes.BIGINT))))

  val INCOME_BAND_100 = new TableStats(20L, Map[String, ColumnStats](
    "ib_income_band_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "ib_lower_bound" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("190001",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "ib_upper_bound" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("200000",DataTypes.BIGINT), convertToNumber("10000",DataTypes.BIGINT))))

  val ITEM_100 = new TableStats(204000L, Map[String, ColumnStats](
    "i_units" -> new ColumnStats(22L, 0L, 4.186769607843138D, 7, null, null),
    "i_brand" -> new ColumnStats(709L, 0L, 16.114941176470587D, 22, null, null),
    "i_rec_start_date" -> new ColumnStats(4L, 545L, 12.0D, 12, null, null),
    "i_rec_end_date" -> new ColumnStats(3L, 102000L, 12.0D, 12, null, null),
    "i_manufact_id" -> new ColumnStats(996L, 528L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_manager_id" -> new ColumnStats(100L, 540L, 8.0D, 8, convertToNumber("100",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_container" -> new ColumnStats(2L, 0L, 6.981058823529412D, 7, null, null),
    "i_formulation" -> new ColumnStats(152874L, 0L, 19.950392156862744D, 20, null, null),
    "i_product_name" -> new ColumnStats(204162L, 0L, 22.285906862745097D, 30, null, null),
    "i_item_desc" -> new ColumnStats(148652L, 0L, 100.00748039215686D, 200, null, null),
    "i_category_id" -> new ColumnStats(10L, 536L, 8.0D, 8, convertToNumber("10",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_item_id" -> new ColumnStats(102033L, 0L, 16.0D, 16, null, null),
    "i_class" -> new ColumnStats(99L, 0L, 7.758583333333333D, 15, null, null),
    "i_current_price" -> new ColumnStats(9085L, 517L, 12.0D, 12, convertToNumber("99.99",DataTypes.DECIMAL(7, 2)), convertToNumber("0.09",DataTypes.DECIMAL(7, 2))),
    "i_category" -> new ColumnStats(11L, 0L, 5.890460784313725D, 11, null, null),
    "i_brand_id" -> new ColumnStats(953L, 512L, 8.0D, 8, convertToNumber("10016017",DataTypes.BIGINT), convertToNumber("1001001",DataTypes.BIGINT)),
    "i_item_sk" -> new ColumnStats(203740L, 0L, 8.0D, 8, convertToNumber("204000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_manufact" -> new ColumnStats(999L, 0L, 11.273299019607844D, 15, null, null),
    "i_size" -> new ColumnStats(8L, 0L, 4.313754901960785D, 11, null, null),
    "i_color" -> new ColumnStats(93L, 0L, 5.3610147058823525D, 10, null, null),
    "i_class_id" -> new ColumnStats(16L, 531L, 8.0D, 8, convertToNumber("16",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "i_wholesale_cost" -> new ColumnStats(6566L, 527L, 12.0D, 12, convertToNumber("89.54",DataTypes.DECIMAL(7, 2)), convertToNumber("0.02",DataTypes.DECIMAL(7, 2)))))

  val PROMOTION_100 = new TableStats(1000L, Map[String, ColumnStats](
    "p_channel_radio" -> new ColumnStats(2L, 0L, 0.988D, 1, null, null),
    "p_item_sk" -> new ColumnStats(983L, 16L, 8.0D, 8, convertToNumber("203752",DataTypes.BIGINT), convertToNumber("196",DataTypes.BIGINT)),
    "p_channel_catalog" -> new ColumnStats(2L, 0L, 0.986D, 1, null, null),
    "p_response_target" -> new ColumnStats(1L, 15L, 8.0D, 8, convertToNumber("1",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "p_start_date_sk" -> new ColumnStats(574L, 15L, 8.0D, 8, convertToNumber("2450915",DataTypes.BIGINT), convertToNumber("2450095",DataTypes.BIGINT)),
    "p_discount_active" -> new ColumnStats(2L, 0L, 0.991D, 1, null, null),
    "p_promo_id" -> new ColumnStats(1003L, 0L, 16.0D, 16, null, null),
    "p_promo_name" -> new ColumnStats(11L, 0L, 3.956D, 5, null, null),
    "p_cost" -> new ColumnStats(1L, 14L, 12.0D, 12, convertToNumber("1000.00",DataTypes.DECIMAL(15, 2)), convertToNumber("1000.00",DataTypes.DECIMAL(15, 2))),
    "p_purpose" -> new ColumnStats(2L, 0L, 6.923D, 7, null, null),
    "p_channel_dmail" -> new ColumnStats(3L, 0L, 0.988D, 1, null, null),
    "p_channel_press" -> new ColumnStats(2L, 0L, 0.989D, 1, null, null),
    "p_end_date_sk" -> new ColumnStats(583L, 10L, 8.0D, 8, convertToNumber("2450970",DataTypes.BIGINT), convertToNumber("2450100",DataTypes.BIGINT)),
    "p_channel_email" -> new ColumnStats(2L, 0L, 0.984D, 1, null, null),
    "p_channel_tv" -> new ColumnStats(2L, 0L, 0.983D, 1, null, null),
    "p_promo_sk" -> new ColumnStats(996L, 0L, 8.0D, 8, convertToNumber("1000",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "p_channel_event" -> new ColumnStats(2L, 0L, 0.986D, 1, null, null),
    "p_channel_demo" -> new ColumnStats(2L, 0L, 0.99D, 1, null, null),
    "p_channel_details" -> new ColumnStats(984L, 0L, 39.704D, 60, null, null)))

  val REASON_100 = new TableStats(55L, Map[String, ColumnStats](
    "r_reason_sk" -> new ColumnStats(55L, 0L, 8.0D, 8, convertToNumber("55",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "r_reason_id" -> new ColumnStats(55L, 0L, 16.0D, 16, null, null),
    "r_reason_desc" -> new ColumnStats(54L, 0L, 13.781818181818181D, 43, null, null)))

  val SHIP_MODE_100 = new TableStats(20L, Map[String, ColumnStats](
    "sm_type" -> new ColumnStats(6L, 0L, 7.5D, 9, null, null),
    "sm_ship_mode_id" -> new ColumnStats(20L, 0L, 16.0D, 16, null, null),
    "sm_ship_mode_sk" -> new ColumnStats(20L, 0L, 8.0D, 8, convertToNumber("20",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "sm_contract" -> new ColumnStats(20L, 0L, 10.05D, 18, null, null),
    "sm_code" -> new ColumnStats(4L, 0L, 4.35D, 7, null, null),
    "sm_carrier" -> new ColumnStats(20L, 0L, 6.65D, 14, null, null)))

  val STORE_100 = new TableStats(402L, Map[String, ColumnStats](
    "s_country" -> new ColumnStats(2L, 0L, 12.935323383084578D, 13, null, null),
    "s_tax_precentage" -> new ColumnStats(12L, 2L, 12.0D, 12, convertToNumber("0.11",DataTypes.DECIMAL(5, 2)), convertToNumber("0.00",DataTypes.DECIMAL(5, 2))),
    "s_market_desc" -> new ColumnStats(308L, 0L, 58.89303482587065D, 100, null, null),
    "s_store_sk" -> new ColumnStats(403L, 0L, 8.0D, 8, convertToNumber("402",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "s_city" -> new ColumnStats(19L, 0L, 9.303482587064677D, 15, null, null),
    "s_store_id" -> new ColumnStats(200L, 0L, 16.0D, 16, null, null),
    "s_suite_number" -> new ColumnStats(75L, 0L, 7.855721393034826D, 9, null, null),
    "s_company_id" -> new ColumnStats(1L, 2L, 8.0D, 8, convertToNumber("1",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "s_store_name" -> new ColumnStats(10L, 0L, 3.987562189054726D, 5, null, null),
    "s_floor_space" -> new ColumnStats(299L, 2L, 8.0D, 8, convertToNumber("9965303",DataTypes.BIGINT), convertToNumber("5023886",DataTypes.BIGINT)),
    "s_street_number" -> new ColumnStats(269L, 0L, 2.8606965174129355D, 3, null, null),
    "s_street_type" -> new ColumnStats(21L, 0L, 4.211442786069652D, 9, null, null),
    "s_number_employees" -> new ColumnStats(98L, 0L, 8.0D, 8, convertToNumber("300",DataTypes.BIGINT), convertToNumber("200",DataTypes.BIGINT)),
    "s_company_name" -> new ColumnStats(2L, 0L, 6.982587064676617D, 7, null, null),
    "s_division_name" -> new ColumnStats(2L, 0L, 6.982587064676617D, 7, null, null),
    "s_zip" -> new ColumnStats(93L, 0L, 5.0D, 5, null, null),
    "s_hours" -> new ColumnStats(3L, 0L, 7.166666666666667D, 8, null, null),
    "s_manager" -> new ColumnStats(305L, 0L, 12.676616915422885D, 21, null, null),
    "s_market_manager" -> new ColumnStats(299L, 0L, 12.614427860696518D, 20, null, null),
    "s_geography_class" -> new ColumnStats(2L, 0L, 6.965174129353234D, 7, null, null),
    "s_gmt_offset" -> new ColumnStats(2L, 2L, 12.0D, 12, convertToNumber("-5.00",DataTypes.DECIMAL(5, 2)), convertToNumber("-6.00",DataTypes.DECIMAL(5, 2))),
    "s_state" -> new ColumnStats(10L, 0L, 1.9900497512437811D, 2, null, null),
    "s_street_name" -> new ColumnStats(263L, 0L, 8.582089552238806D, 18, null, null),
    "s_closed_date_sk" -> new ColumnStats(72L, 299L, 8.0D, 8, convertToNumber("2451299",DataTypes.BIGINT), convertToNumber("2450824",DataTypes.BIGINT)),
    "s_rec_start_date" -> new ColumnStats(4L, 2L, 12.0D, 12, null, null),
    "s_county" -> new ColumnStats(10L, 0L, 14.129353233830846D, 17, null, null),
    "s_division_id" -> new ColumnStats(1L, 2L, 8.0D, 8, convertToNumber("1",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "s_rec_end_date" -> new ColumnStats(3L, 201L, 12.0D, 12, null, null),
    "s_market_id" -> new ColumnStats(10L, 1L, 8.0D, 8, convertToNumber("10",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT))))

  val TIME_DIM_100 = new TableStats(86400L, Map[String, ColumnStats](
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

  val WAREHOUSE_100 = new TableStats(15L, Map[String, ColumnStats](
    "w_state" -> new ColumnStats(9L, 0L, 2.0D, 2, null, null),
    "w_gmt_offset" -> new ColumnStats(2L, 0L, 12.0D, 12, convertToNumber("-5.00",DataTypes.DECIMAL(5, 2)), convertToNumber("-6.00",DataTypes.DECIMAL(5, 2))),
    "w_warehouse_id" -> new ColumnStats(15L, 0L, 16.0D, 16, null, null),
    "w_county" -> new ColumnStats(9L, 0L, 13.933333333333334D, 17, null, null),
    "w_zip" -> new ColumnStats(15L, 0L, 5.0D, 5, null, null),
    "w_city" -> new ColumnStats(10L, 0L, 9.0D, 13, null, null),
    "w_country" -> new ColumnStats(1L, 0L, 13.0D, 13, null, null),
    "w_warehouse_name" -> new ColumnStats(15L, 0L, 14.866666666666667D, 19, null, null),
    "w_street_type" -> new ColumnStats(11L, 0L, 4.266666666666667D, 9, null, null),
    "w_street_name" -> new ColumnStats(15L, 0L, 8.866666666666667D, 18, null, null),
    "w_street_number" -> new ColumnStats(15L, 0L, 2.8666666666666667D, 3, null, null),
    "w_warehouse_sk" -> new ColumnStats(15L, 0L, 8.0D, 8, convertToNumber("15",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "w_warehouse_sq_ft" -> new ColumnStats(15L, 0L, 8.0D, 8, convertToNumber("827680",DataTypes.BIGINT), convertToNumber("50133",DataTypes.BIGINT)),
    "w_suite_number" -> new ColumnStats(13L, 0L, 8.266666666666667D, 9, null, null)))

  val WEB_PAGE_100 = new TableStats(2040L, Map[String, ColumnStats](
    "wp_image_count" -> new ColumnStats(7L, 39L, 8.0D, 8, convertToNumber("7",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wp_char_count" -> new ColumnStats(1334L, 24L, 8.0D, 8, convertToNumber("8112",DataTypes.BIGINT), convertToNumber("368",DataTypes.BIGINT)),
    "wp_autogen_flag" -> new ColumnStats(3L, 0L, 0.9852941176470589D, 1, null, null),
    "wp_creation_date_sk" -> new ColumnStats(132L, 24L, 8.0D, 8, convertToNumber("2450815",DataTypes.BIGINT), convertToNumber("2450672",DataTypes.BIGINT)),
    "wp_link_count" -> new ColumnStats(24L, 31L, 8.0D, 8, convertToNumber("25",DataTypes.BIGINT), convertToNumber("2",DataTypes.BIGINT)),
    "wp_rec_start_date" -> new ColumnStats(4L, 38L, 12.0D, 12, null, null),
    "wp_web_page_id" -> new ColumnStats(1021L, 0L, 16.0D, 16, null, null),
    "wp_rec_end_date" -> new ColumnStats(3L, 1020L, 12.0D, 12, null, null),
    "wp_access_date_sk" -> new ColumnStats(100L, 30L, 8.0D, 8, convertToNumber("2452648",DataTypes.BIGINT), convertToNumber("2452548",DataTypes.BIGINT)),
    "wp_customer_sk" -> new ColumnStats(519L, 1424L, 8.0D, 8, convertToNumber("1999384",DataTypes.BIGINT), convertToNumber("3861",DataTypes.BIGINT)),
    "wp_web_page_sk" -> new ColumnStats(2051L, 0L, 8.0D, 8, convertToNumber("2040",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "wp_max_ad_count" -> new ColumnStats(5L, 35L, 8.0D, 8, convertToNumber("4",DataTypes.BIGINT), convertToNumber("0",DataTypes.BIGINT)),
    "wp_type" -> new ColumnStats(8L, 0L, 6.332843137254902D, 9, null, null),
    "wp_url" -> new ColumnStats(2L, 0L, 17.68235294117647D, 18, null, null)))

  val WEB_SITE_100 = new TableStats(24L, Map[String, ColumnStats](
    "web_market_manager" -> new ColumnStats(19L, 0L, 13.375D, 16, null, null),
    "web_country" -> new ColumnStats(1L, 0L, 13.0D, 13, null, null),
    "web_open_date_sk" -> new ColumnStats(12L, 0L, 8.0D, 8, convertToNumber("2450807",DataTypes.BIGINT), convertToNumber("2450628",DataTypes.BIGINT)),
    "web_street_type" -> new ColumnStats(14L, 0L, 4.416666666666667D, 9, null, null),
    "web_zip" -> new ColumnStats(13L, 0L, 5.0D, 5, null, null),
    "web_gmt_offset" -> new ColumnStats(2L, 0L, 12.0D, 12, convertToNumber("-5.00",DataTypes.DECIMAL(5, 2)), convertToNumber("-6.00",DataTypes.DECIMAL(5, 2))),
    "web_street_number" -> new ColumnStats(15L, 0L, 3.0D, 3, null, null),
    "web_state" -> new ColumnStats(9L, 0L, 2.0D, 2, null, null),
    "web_suite_number" -> new ColumnStats(19L, 0L, 8.25D, 9, null, null),
    "web_rec_end_date" -> new ColumnStats(3L, 12L, 12.0D, 12, null, null),
    "web_close_date_sk" -> new ColumnStats(8L, 4L, 8.0D, 8, convertToNumber("2447131",DataTypes.BIGINT), convertToNumber("2443328",DataTypes.BIGINT)),
    "web_company_name" -> new ColumnStats(5L, 0L, 3.75D, 5, null, null),
    "web_manager" -> new ColumnStats(16L, 0L, 11.958333333333334D, 17, null, null),
    "web_mkt_class" -> new ColumnStats(18L, 0L, 37.208333333333336D, 49, null, null),
    "web_mkt_id" -> new ColumnStats(6L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "web_name" -> new ColumnStats(4L, 0L, 6.0D, 6, null, null),
    "web_tax_percentage" -> new ColumnStats(8L, 0L, 12.0D, 12, convertToNumber("0.12",DataTypes.DECIMAL(5, 2)), convertToNumber("0.01",DataTypes.DECIMAL(5, 2))),
    "web_company_id" -> new ColumnStats(5L, 0L, 8.0D, 8, convertToNumber("6",DataTypes.BIGINT), convertToNumber("2",DataTypes.BIGINT)),
    "web_site_sk" -> new ColumnStats(24L, 0L, 8.0D, 8, convertToNumber("24",DataTypes.BIGINT), convertToNumber("1",DataTypes.BIGINT)),
    "web_mkt_desc" -> new ColumnStats(18L, 0L, 59.708333333333336D, 95, null, null),
    "web_rec_start_date" -> new ColumnStats(4L, 0L, 12.0D, 12, null, null),
    "web_class" -> new ColumnStats(1L, 0L, 7.0D, 7, null, null),
    "web_city" -> new ColumnStats(13L, 0L, 9.833333333333334D, 14, null, null),
    "web_county" -> new ColumnStats(9L, 0L, 14.291666666666666D, 17, null, null),
    "web_site_id" -> new ColumnStats(12L, 0L, 16.0D, 16, null, null),
    "web_street_name" -> new ColumnStats(22L, 0L, 7.458333333333333D, 15, null, null)))

  def loadStats(table: String): TableStats = {
    table match {
      case "catalog_sales" => CATALOG_SALES_100
      case "catalog_returns" => CATALOG_RETURNS_100
      case "inventory" => INVENTORY_100
      case "store_sales" => STORE_SALES_100
      case "store_returns" => STORE_RETURNS_100
      case "web_sales" => WEB_SALES_100
      case "web_returns" => WEB_RETURNS_100
      case "call_center" => CALL_CENTER_100
      case "catalog_page" => CATALOG_PAGE_100
      case "customer" => CUSTOMER_100
      case "customer_address" => CUSTOMER_ADDRESS_100
      case "customer_demographics" => CUSTOMER_DEMOGRAPHICS_100
      case "date_dim" => DATE_DIM_100
      case "household_demographics" => HOUSEHOLD_DEMOGRAPHICS_100
      case "income_band" => INCOME_BAND_100
      case "item" => ITEM_100
      case "promotion" => PROMOTION_100
      case "reason" => REASON_100
      case "ship_mode" => SHIP_MODE_100
      case "store" => STORE_100
      case "time_dim" => TIME_DIM_100
      case "warehouse" => WAREHOUSE_100
      case "web_page" => WEB_PAGE_100
      case "web_site" => WEB_SITE_100
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


