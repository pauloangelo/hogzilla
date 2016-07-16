/*
 * Copyright (C) 2015-2015 Paulo Angelo Alves Resende <pa@pauloangelo.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License Version 2 as
 * published by the Free Software Foundation.  You may not use, modify or
 * distribute this program under any other version of the GNU General
 * Public License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

package org.hogzilla.hbase


/**
 * @author pa
 */

import scala.math.random
import java.lang.Math
import org.apache.spark._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.regression.{LabeledPoint,LinearRegressionModel,LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter
import java.util.ArrayList
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter
import scala.collection.mutable.HashSet


object HogHBaseReputation {

  // Ex: MX, whitelist
	def getReputationList(listName:String, listType:String):Set[String] =
	{
		val list =  new HashSet[String]


	  val filters: ArrayList[Filter] = new ArrayList();

		val colValFilter1 = new SingleColumnValueFilter(Bytes.toBytes("rep"), Bytes.toBytes("list_type"),
				CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(listType)))
		colValFilter1.setFilterIfMissing(false);

		val colValFilter2 = new SingleColumnValueFilter(Bytes.toBytes("rep"), Bytes.toBytes("list"),
				CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(listName)))
		colValFilter2.setFilterIfMissing(false);

		filters.add(colValFilter1);
		filters.add(colValFilter2);

		val filterList = new FilterList( FilterList.Operator.MUST_PASS_ALL, filters);
		val scan = new Scan()
		scan.setFilter(filterList)
    
		val it = HogHBaseRDD.hogzilla_reputation.getScanner(scan).iterator()
		
    while(it.hasNext())
		{
      list.add( Bytes.toString(it.next().getValue(Bytes.toBytes("rep"),Bytes.toBytes("ip"))) )
		}
    
    list.toSet

	}

}