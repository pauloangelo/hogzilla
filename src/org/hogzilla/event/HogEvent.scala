package org.hogzilla.event


import java.util.HashMap
import java.util.Map
import org.apache.hadoop.hbase.client.RowMutations
import org.hogzilla.hbase.HogHBaseRDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put


class HogEvent(flow:Map[String,String]) 
{
	var sensorid:Int=0
			var eventsecond:Int=0
			var signatureid:Int=0
			var generatorid:Int=0
			var priorityid:Int=0
			var text:String=""
			var data:Map[String,String]=new HashMap()

   def alert()
   {
	   println(flow.get("flow:host_server_name"))    


	  // val mutation = new RowMutations()
     val put = new Put(Bytes.toBytes(flow.get("flow:id")))
     put.add(Bytes.toBytes("event"), Bytes.toBytes("text"), Bytes.toBytes(text))
     //mutation.add(put)
     HogHBaseRDD.hogzilla_events.put(put)


	/*
	 * iph
	 * sig info
	 * udph
	 * refs url -> hog/xxx/xxx
	 * payload -> txt
	 *

 select * from event limit 2;
+-----+----------+-----------+-------------------+-------------+---------+-------------+------+------------------+---------------------+----------+
| sid | cid      | signature | classification_id | users_count | user_id | notes_count | type | number_of_events | timestamp           | id       |
+-----+----------+-----------+-------------------+-------------+---------+-------------+------+------------------+---------------------+----------+
|   1 | 16054689 |       135 |              NULL |           0 |    NULL |           0 |    1 |                0 | 2015-09-17 08:57:29 | 16054689 |
|   1 | 16054690 |       135 |              NULL |           0 |    NULL |           0 |    1 |                0 | 2015-09-17 08:57:29 | 16054690 |
+-----+----------+-----------+-------------------+-------------+---------+-------------+------+------------------+---------------------+----------+

 select * from data limit 1;
+-----+-----+----------------------------------------------------------------------------------------------------------+
| sid | cid | data_payload                                                                                             |
+-----+-----+----------------------------------------------------------------------------------------------------------+
|   1 |   3 | 6A3934D41694F0F674D7FEC611C8BB9C2DFF9A6D9AAF7D9CDBA32673E4B757F88BEB0E4948AFF88D2A2CC70B80D6B296E2FDE8B3 |
+-----+-----+----------------------------------------------------------------------------------------------------------+
  select * from udphdr limit 1;
+-----+-----+-----------+-----------+---------+----------+
| sid | cid | udp_sport | udp_dport | udp_len | udp_csum |
+-----+-----+-----------+-----------+---------+----------+
|   1 |   4 |     57787 |        53 |      48 |    46126 |
+-----+-----+-----------+-----------+---------+----------+

select * from reference limit 10;
+--------+---------------+---------+
| ref_id | ref_system_id | ref_tag |
+--------+---------------+---------+
|      1 |             1 | 219     |
|      2 |             1 | 480     |
|   6214 |             8 | www.virustotal.com/file/4DA4DB7C7547859B48FCEE2D4E0827983161F3FA04FA87BBFDBA558F9F80F74F/analysis/                                                                                                      |

 select * from iphdr limit 1;
+-----+-----+------------+-----------+--------+---------+--------+--------+-------+----------+--------+--------+----------+---------+
| sid | cid | ip_src     | ip_dst    | ip_ver | ip_hlen | ip_tos | ip_len | ip_id | ip_flags | ip_off | ip_ttl | ip_proto | ip_csum |
+-----+-----+------------+-----------+--------+---------+--------+--------+-------+----------+--------+--------+----------+---------+
|   1 |   1 | 1249343050 | 167838309 |      4 |       5 |      0 |     40 | 14308 |        0 |      0 |    246 |        6 |   32708 |
+-----+-----+------------+-----------+--------+---------+--------+--------+-------+----------+--------+--------+----------+---------+
1 row in set (0.01 sec)

> select * from notes;
+----+------+----------+---------+-------+---------------------+---------------------+
| id | sid  | cid      | user_id | body  | created_at          | updated_at          |
+----+------+----------+---------+-------+---------------------+---------------------+
|  2 |    1 | 17307549 |       1 | Teste | 2015-09-18 22:18:19 | 2015-09-18 22:18:19 |
+----+------+----------+---------+-------+---------------------+---------------------+

> select * from signature limit 1;
+--------+--------------+----------------------------------------------------------------------+--------------+---------+---------+---------+--------------+
| sig_id | sig_class_id | sig_name                                                             | sig_priority | sig_rev | sig_sid | sig_gid | events_count |
+--------+--------------+----------------------------------------------------------------------+--------------+---------+---------+---------+--------------+
|      1 |            0 | dnp3: DNP3 Application-Layer Fragment uses a reserved function code. |            0 |       1 |       6 |     145 |            0 |
+--------+--------------+----------------------------------------------------------------------+--------------+---------+---------+---------+--------------+

> select * from sig_reference limit 1;
+--------+---------+--------+
| sig_id | ref_seq | ref_id |
+--------+---------+--------+
|    509 |       1 |   5460 |
+--------+---------+--------+
1 row in set (0.00 sec)


	 */

}
}

