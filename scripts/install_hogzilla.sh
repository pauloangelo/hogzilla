#!/bin/bash
#
# Copyright (C) 2015-2016 Paulo Angelo Alves Resende <pa@pauloangelo.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License Version 2 as
# published by the Free Software Foundation.  You may not use, modify or
# distribute this program under any other version of the GNU General
# Public License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#


# Load libs
. bsfl
. my_funcs

TMP_FILE="/tmp/.hzinstallation.temp"
HZURL="http://ids-hogzilla.org"

whoami | grep pa &>/dev/null
if [ $? -gt 0 ] ; then
  msg_fail "Logged as root!"
  die 1 "Root needed!"
else
  msg_ok "Logged as root!"
fi

file_exists "/etc/debian_version"
if [ $? -gt 0 ] ; then
  msg_fail "This script must run on Debian and you are not running on Debian!"
  die 1 "You should use Debian Linux to run this script! If you want to run Hogzilla IDS on a different OS, you should do it manually. See the installation guide at http://ids-hogzilla.org "
else
  msg_ok "Running on Debian."
fi

package_installed_cmd "dialog" "dialog" "Dialog"

sleep 1
dialog --title "Hogzilla Installation Script" \
       --yesno \
"\n\n
This is an automate installation script for Hogzilla IDS.\n 
It should do the following:\n 
   1) Check your system;\n
   2) Create user hogzilla;\n
   3) Install Java, Hadoop, HBase, Apache-Spark, SflowTool, \n
      SFlow2Hz, Thrift, Compose and PigTail;\n
   4) Configure Hadoop, HBase and PigTail;\n
   5) Create scripts;\n
   6) Create SSH-Keys; and\n
   7) Include lines into /etc/rc.local .\n
   \n
You will be requested to inform:\n
   - GrayLog URL\n
   - Proxy URL for Internet access (if needed)\n
   - Default path for Hadoop data (recommended > 50G)\n
\n
                  DO YOU WANT TO CONTINUE?
\n \n" 0 0

if [ $? -gt 0 ] ; then
  echo ""
  msg_warning "OK. We are finishing now!"
else
  echo ""
  msg_ok "You chose to continue! Let's do it!!!"
fi


sleep 1
dialog --inputbox \
"Enter your proxy URL (ex. http://10.1.1.1:8080) or \n
let it blank for no proxy :" 20 70 $http_proxy 2> $TMP_FILE
PROXY=`head -n1 $TMP_FILE`
export https_proxy=$PROXY
export http_proxy=$PROXY

dialog --inputbox \
"Enter your GrayLog URL (ex. http://graylog.example.com:9090):" 20 70 2> $TMP_FILE
GRAYLOG=`head -n1 $TMP_FILE`

dialog --inputbox \
"Enter the Hadoop Data path:" 20 70 "/home/hogzilla/hadoop_data" 2> $TMP_FILE
HADOOPDATA=`head -n1 $TMP_FILE`

package_installed_cmd "wget" "wget" "Wget"
package_installed_cmd "awk" "gawk" "gawk"
package_installed_cmd "sed" "sed" "sed"
package_installed "lynx" "lynx"

wget -o /dev/null $GRAYLOG 
if [ $? -gt 0 ] ; then
  msg_fail "Could not access GrayLog on $GRAYLOG!"
  die 1 "GrayLog could not be accessed. Check the entered URL and try again!"
else
  msg_ok "Accessing GrayLog correctly on $GRAYLOG!"
fi

wget -o /dev/null $HZURL
if [ $? -gt 0 ] ; then
  msg_fail "Could not access Internet!"
  die 1 "Internet could not be reached! Check the entered information and try again!"
else
  msg_ok "Internet could be reached!"
fi

SPACE=`df $HADOOPDATA | tail -n1 | awk '{print $4}' | sed 's/\([0-9]*\).*/\1/'`
if [ $SPACE -lt 50000000 ] ; then
  msg_warning "You have less than 50GB on $HADOOPDATA . Hogzilla may not run properly. Nevertheless installation will continue."
else
  msg_ok "You have enough space on $HADOOPDATA !"
fi

#Install dependencies
for pkg in "php5-dev" "php5-cli" "phpunit" ; do
    package_installed $pkg $pkg
done
 
# Faz apenas se não fez cada um
cmd useradd hogzilla
cmd mkdir -p $HADOOPDATA
cmd chown hogzilla. $HADOOPDATA
cmd su hogzilla -c "ssh-keygen -t rsa -f /home/hogzilla/.ssh/id_rsa -q -N '' "
cmd su hogzilla -c "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"
cmd su hogzilla -c "chmod 0600 ~/.ssh/authorized_keys"


# Check JAVA

#Se não existe java, pergunta
JAVA=true

dialog --yesno \
"Quer tentar instalar o JAVA?" 20 70

if [ $? -gt 0 ] ; then
  echo ""
  msg_warning "OK. You will need to install Oracle's Java later!"
  JAVA=false
else
  echo ""
  msg_ok "You chose to continue! Let's do it!!!"
  cmd echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" | tee -a /etc/apt/sources.list
  cmd echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" | tee -a /etc/apt/sources.list
  cmd apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886
  cmd apt-get update
  cmd apt-get install oracle-java6-installer
  cmd apt-get install oracle-java6-set-default
  #Check java again  
  JAVA=true
fi


sleep 1

# Instala apenas se não tem
# Check MD5
# Hadoop
# Install
# Check
cmd su hogzilla -c "mkdir /home/hogzilla/app"
cmd su hogzilla -c "cd /home/hogzilla/app"
cmd su hogzilla -c "wget 'http://www.us.apache.org/dist/hadoop/common/stable/hadoop-2.7.1.tar.gz'" # Colocar diretório de saída
cmd su hogzilla -c "tar xzvf hadoop-2.7.1.tar.gz" # Colocar diretório de saída
cmd su hogzilla -c "mv hadoop-2.7.1 /home/hogzilla/hadoop"
cmd su hogzilla -c "wget -c 'http://www.us.apache.org/dist/hbase/stable/hbase-1.1.2-bin.tar.gz'"
cmd su hogzilla -c "tar xzvf hbase-1.1.2-bin.tar.gz" # Colocar diretório de saída
cmd su hogzilla -c "mv hbase-1.1.2 /home/hogzilla/hbase"
#Verifica tudo

echo 'export HADOOP_HOME=/home/hogzilla/hadoop' >> ~/.bashrc
echo 'export HADOOP_MAPRED_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_COMMON_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_HDFS_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export YARN_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export HADOOP_INSTALL=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"' >> ~/.bashrc
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
source ~/.bashrc



cd $HADOOP_HOME/etc/hadoop
echo 'export JAVA_HOME=/usr/java/jdk1.7.0_79/' >> hadoop-env.sh

cp -i core-site.xml core-site.xml-original
cp -i hdfs-site.xml hdfs-site.xml-original
cp -i yarn-site.xml yarn-site.xml-original
cp -i mapred-site.xml.template mapred-site.xml


Put the lines below inside “configuration” tags in core-site.xml

   <property>
      <name>fs.default.name</name>
      <value>hdfs://localhost:9000</value>
   </property>

Put the lines below inside “configuration” tags in hdfs-site.xml

   <property>
      <name>dfs.replication</name >
      <value>1</value>
   </property>
   <property>
      <name>dfs.name.dir</name>
      <value>file:///data/hdfs/namenode</value>
   </property>
   <property>
      <name>dfs.data.dir</name>
      <value>file:///data/hdfs/datanode</value>
   </property>

Put the lines below inside “configuration” tags in yarn-site.xml

   <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
   </property>

Put the lines below inside “configuration” tags in mapred-site.xml

   <property>
      <name>mapreduce.framework.name</name>
      <value>yarn</value>
   </property>

Initiate HDFS and Start Hadoop

hdfs namenode -format
start-dfs.sh
start-yarn.sh

Configure HBase

cd /home/hogzilla/hbase/conf
cp -i hbase-env.sh hbase-env.sh-original
cp -i hbase-site.xml hbase-site.xml-original
echo 'export JAVA_HOME=/usr/java/jdk1.7.0_79/' >> hbase-env.sh

Put the lines below inside “configuration” tags in hbase-site.xml

<property>
    <name>zookeeper.znode.rootserver</name>
    <value>localhost</value>
</property>
<property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
</property>
<property>
    <name>hbase.rootdir</name>
    <value>hdfs://localhost:9000/hbase</value>
</property>
<property>
    <!-- <name>hbase.regionserver.lease.period</name> -->
    <name>hbase.client.scanner.timeout.period</name>
    <value>900000</value> <!-- 900 000, 15 minutes -->
</property>
<property>
    <name>hbase.rpc.timeout</name>
    <value>900000</value> <!-- 15 minutes -->
</property>
<property>
    <name>hbase.thrift.connection.max-idletime</name>
    <value>1800000</value>
</property>




cd /home/hogzilla/hbase
./bin/start-hbase.sh
./bin/hbase-daemon.sh start thrift

Create Hogzilla tables in HBase

./bin/hbase shell

Inside HBase Shell

# Baixar direto do GIT
create 'hogzilla_flows','flow','event'
create 'hogzilla_events','event'
create 'hogzilla_sensor','sensor'
create 'hogzilla_signatures','signature'

More variables in ./.bashrc

echo 'export CLASSPATH=$CLASSPATH:/home/hogzilla/hbase/lib/*' >> ~/.bashrc
source ~/.bashrc




cd /home/hogzilla/app
wget http://mirror.nbtelecom.com.br/apache/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
tar xzvf spark-1.6.0-bin-hadoop2.6.tgz
mv spark-1.6.0-bin-hadoop2.6 /home/hogzilla/spark

Configure Apache Spark

cd /home/hogzilla/spark/conf
cp spark-env.sh.template spark-env.sh
echo 'SPARK_DRIVER_MEMORY=1G' >> spark-env.sh

Start Apache Spark

cd /home/hogzilla
./spark/sbin/start-master.sh
./spark/sbin/start-slaves.sh

cd /home/hogzilla
wget http://ids-hogzilla.org/downloads/Hogzilla-v0.5.1-alpha.jar
mv Hogzilla-v0.5.1-alpha.jar Hogzilla.jar

chmod 755 hogzilla.sh
./hogzilla.sh &


Download Pigtail

mkdir /root/app
cd /root/app
apt-get install git
git clone https://github.com/pauloangelo/pigtail.git
mv pigtail/pigtail.php /root
mkdir /usr/lib/php/Thrift/Packages/
mv pigtail/gen-php/Hbase/  /usr/lib/php/Thrift/Packages/

apt-get install php5-mysql
cd /root

php ./pigtail.php >&/dev/null&


su hogzilla -c "/home/hogzilla/hadoop/sbin/start-dfs.sh"
su hogzilla -c "/home/hogzilla/hadoop/sbin/start-yarn.sh"
# Start HBase
su hogzilla -c "/home/hogzilla/hbase/bin/start-hbase.sh"
su hogzilla -c "/home/hogzilla/hbase/bin/hbase-daemon.sh start thrift"

# HBase
# Apache-spark
# sFlow collector
# Thrift
# compose
# Pigtail

# Configure HBase, create tables

# hogzilla.sh
# startallhz.sh: runpig.sh, runsflow2hz.sh
# rc.local
