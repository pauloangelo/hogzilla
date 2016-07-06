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

define("DEBUG",true);

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
if(DEBUG) {  echo "Parse options\n" ;}
$options = getopt("t:n:f:");
$listType=@$options["t"];
$listName=@$options["n"];
$listFile=@$options["f"];

if(strlen($listType) ==0 || strlen($listName) ==0 || strlen($listFile) ==0  )
{
   echo "Usage: php updateReputationList.php -t ListType -n ListName -f file \n";
   echo "Examples: php updateReputationList.php -t whitelist -n MX      -f file_one_ip_per_line.txt \n";
   echo "          php updateReputationList.php -t whitelist -n TTalker -f file_one_ip_per_line.txt \n";
   exit;
}


// Open file
if(DEBUG) {  echo "Open file\n" ;}
$fileHandle = fopen($listFile, "r");
if(!$fileHandle) {
  echo "Error opening file $listFile .";
  exit;
}

// Open connections
if(DEBUG) {  echo "Open connection\n" ;}
$transport->open();

// Scan+Filter on HBase
$filter = array();
$filter[] = "SingleColumnValueFilter('rep', 'list_type', =, 'binary:".$listType."')";
$filter[] = "SingleColumnValueFilter('rep', 'list',      =, 'binary:".$listName."')";
$filterString = implode(" AND ", $filter);
$scanFilter = new Hbase\TScan();
$scanFilter->filterString = $filterString;
$scanner = $client->scannerOpenWithScan("hogzilla_reputation", $scanFilter, array());

// Delete rows, iterating
if(DEBUG) {  echo "Deleting current list from HBase\n" ;}
try
{
    while (true) 
    {
        $row=$client->scannerGet($scanner);
        if(sizeof($row)==0) break;
        if(DEBUG) {  
                     $values = $row[0]->columns;
                     $ip     = $values["rep:ip"]->value;
                     echo "Deleting $ip from list $listName/$listType\n" ;
                  }
        $client->deleteAllRow("hogzilla_reputation", $row[0]->row, array());
    }
    $client->scannerClose($scanner);
    
    // Iterate file
    while (($ip = fgets($fileHandle)) !== false) 
    {
       // Parse
       preg_replace( "/\r|\n/", "", $ip );
       $ip=trim($ip);
       // Create mutation
       $mutations = array();
       $dataIP    = array(
            'column' => "rep:ip",
            'value'  => $ip
       );
       $dataListName = array('column' => "rep:list",        'value'  => $listName );
       $dataListType = array('column' => "rep:list_type",   'value'  => $listType );
       $dataListDesc = array('column' => "rep:description", 'value'  => "" );
       $mutations[] = new Hbase\Mutation($dataIP);
       $mutations[] = new Hbase\Mutation($dataListName);
       $mutations[] = new Hbase\Mutation($dataListType);
       $mutations[] = new Hbase\Mutation($dataListDesc);
       // Insert mutations
       $client->mutateRow("hogzilla_reputation", $ip, $mutations, array());
    }
} catch(Exception $e) 
{
  echo 'ERROR: ',  $e->getMessage(), "\n";
}

// Close file
fclose($fileHandle);

// Close connections (HBase)
$transport->close();

?>
