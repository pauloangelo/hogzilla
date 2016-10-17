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

package org.hogzilla.cluster

import org.apache.spark.mllib.linalg.Vector


/** clusterIdx,centroidMain,clusterSize,members.filter(_._1.equals(clusterIdx)).map({_._2})
 * @author pa
 */
case class HogClusterMember(clusterIdx:Int, centroid:List[(Long,Double)], clusterSize:Long, allKeys:List[Long],
                            memberIP:String, ports:Set[Long], frequency_vector:List[(Long,Double)], distance:Double)
{
  
  def formatTitle:String =
 {
     "Group information for "+memberIP
 }
 
}