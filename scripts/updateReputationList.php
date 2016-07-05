<?php
(PHP_SAPI !== 'cli' || isset($_SERVER['HTTP_USER_AGENT'])) && die("Must run in cli mode\n");
/*
* Copyright (C) 2015-2016 Paulo Angelo Alves Resende <pa@pauloangelo.com>
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
* 
* MORE CREDITS
*  - Contribute and put your "Name <email> - Contribution"  here.
* 
* USING THIS SCRIPT
*
* 1. Run 
*     /usr/bin/php updateReputationList.php list
*
* ATTENTION: This PHP script must run in CLI! 
*
* If you have any problems, let us know! 
* See how to get help at http://ids-hogzilla.org/post/community/ 
*/

// Some useful variables
$hbaseHost="hoghbasehost"; /* Host or IP of your HBase  */
$hbasePort=9090;

$GLOBALS['THRIFT_ROOT'] = '/usr/lib/php';

define("DEBUG",false);

// Thrift stuff
require_once($GLOBALS['THRIFT_ROOT'].'/Thrift/ClassLoader/ThriftClassLoader.php');

$classLoader = new Thrift\ClassLoader\ThriftClassLoader();
$classLoader->registerNamespace('Thrift', $GLOBALS['THRIFT_ROOT']);
$classLoader->register();

require_once($GLOBALS['THRIFT_ROOT'].'/Thrift/Transport/TSocket.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Thrift/Transport/TBufferedTransport.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Thrift/Protocol/TBinaryProtocol.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Thrift/Packages/Hbase/Hbase.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Thrift/Packages/Hbase/Types.php');

$socket     = new Thrift\Transport\TSocket($hbaseHost, $hbasePort);
$socket->setSendTimeout(10000);
$socket->setRecvTimeout(20000);
$transport  = new Thrift\Transport\TBufferedTransport($socket);
$protocol   = new Thrift\Protocol\TBinaryProtocol($transport);
$client     = new Hbase\HbaseClient($protocol);


/*
 * BEGIN
 */

// Parse arguments
$listName
$listType

// Open file

// Open connections
if(DEBUG) {  echo "Open connection\n" ;}
$transport->open();

// Scan+Filter on HBase

// Delete rows, iterating

// Iterate file
// Create mutation
// Add mutation to list
// Insert mutations

// Close file
// Close connection




if(DEBUG) {  echo "Insert sensor, if needed\n" ;}
$scanner = $client->scannerOpenWithStop("hogzilla_sensor","","", array("sensor:description","sensor:hostname"), array());
$row=$client->scannerGet($scanner);
if(sizeof($row)==0) { die("Sensor table is empty in HBase\n"); }
saveSensor($row,$con);
$client->scannerClose($scanner);

// Insert Signatures if needed. Get Signature information
if(DEBUG) {  echo "Insert signatures, if needed\n" ;}
$scanner = $client->scannerOpenWithStop("hogzilla_signatures","","",
                    array("signature:class","signature:name","signature:priority",
                          "signature:revision","signature:id","signature:group_id"),
                    array());
while (true) 
{
    $row=$client->scannerGet($scanner);
    if(sizeof($row)==0) break;
    saveSignature($row,$con);
}

$client->scannerClose($scanner);


/*
 * 
 */
if(DEBUG) {  echo "Inside loop\n" ;}
// Open HBase and MySQL connection
if(DEBUG) {  echo "Open connections\n" ;}
$transport->open();

// Get HBase pointer
$scanner = $client->scannerOpenWithStop("hogzilla_events",$startrow,"",
                        array("event:lower_ip","event:upper_ip","event:note","event:signature_id"),
                        array());

// Loop events to insert into MySQL
try
{
    while (true) 
    {
            $row=$client->scannerGet($scanner);
            if(sizeof($row)==0) break;
            $client->deleteAllRow("hogzilla_events", $row[0]->row, array()) ;
    }
} catch(Exception $e) 
{
  echo 'ERROR: ',  $e->getMessage(), "\n";
}

// Close connections (HBase)
$client->scannerClose($scanner);
$transport->close();

?>
