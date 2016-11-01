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

# Variables
HADOOP_VERSION="2.7.3"
HBASE_VERSION="1.2.3"
SPARK_VERSION="2.0.1"
SFLOWTOOL_VERSION="3.39"
TMP_FILE="/tmp/.hzinstallation.temp"
HZURL="http://ids-hogzilla.org"
DEBUG=0

# Load libs
. bsfl
. myFuncs

whoami | grep root &>/dev/null
if [ $? -gt 0 ] ; then
  msg_fail "Logged as root!"
  die 1 "Root needed!"
else
  msg_ok "Logged as root!"
fi

file_exists "/etc/debian_version"
if [ $? -gt 0 ] ; then
  msg_fail "This script must run on Debian and you are not running on Debian!"
  die 1 "You should use Debian Linux to run this script! If you want to run Hogzilla IDS on a different OS, you should do it manually. See the installation guide at $HZURL "
else
  msg_ok "Running on Debian."
fi

package_install_cmd "whiptail -h" "whiptail" "Whiptail"

sleep 1
whiptail --scrolltext --title "Hogzilla Installation Script" \
       --yesno \
"\n[You can scrool the text if needed!]\n
This is an automate installation script for Hogzilla IDS.\n 
It should do the following:\n 
   1) Check your system;\n
   2) Create user hogzilla;\n
   3) Install Java, Hadoop, HBase, Apache-Spark, SflowTool, \n
      SFlow2Hz, Thrift, HzTools, PigTail and some basic pre-requisites;\n
   4) Configure Hadoop, HBase, Apache-Spark and PigTail;\n
   5) Start programs;\n
   6) Create SSH-Keys; and\n
   7) Include lines into /etc/rc.local .\n
   \n
PRE-REQUISITES:\n
   - Debian amd64 (tested on Debian 8.6.0)
   - > 8G RAM
   - > 50G HD
   - > 1 CPU 
   \n
You will be requested to inform:\n
   - GrayLog Server IP\n
   - Proxy URL for Internet access (if needed)\n
   - Default path for Hadoop data (recommended > 50G)\n
\n
                  DO YOU WANT TO CONTINUE?
\n \n" 20 70

if [ $? -gt 0 ] ; then
  echo ""
  msg_warning "OK. We are finishing now!"
  exit
else
  echo ""
  msg_ok "You chose to continue! Let's do it!!!"
fi

sleep 1
whiptail --title "ALERT" --msgbox \
"
In this instalation, HBase, Hadoop, etc. will enable services without authentication.\n
Due to that, the server must be in a network protected by firewalling.\n
" 20 70 

whiptail --inputbox \
"Enter your proxy URL (ex. http://10.1.1.1:8080) or \n
let it blank for no proxy :" 20 70 $http_proxy 2> $TMP_FILE
PROXY=`head -n1 $TMP_FILE`
export https_proxy=$PROXY
export http_proxy=$PROXY

whiptail --inputbox \
"Enter your GrayLog server IP (e.g. 10.1.1.1):" 20 70 "10.1.1.1" 2> $TMP_FILE
GRAYLOGHOST=`head -n1 $TMP_FILE`

whiptail --inputbox \
"Enter the Hadoop Data path (you need at least 50G):" 20 70 "/home/hogzilla/hadoop_data" 2> $TMP_FILE
HADOOPDATA=`head -n1 $TMP_FILE`

whiptail --inputbox \
"Enter your (internal and external) networks' prefixes separated by commas.\n
E.g. If your networks are 10.0.0.0/15 and 1.1.1.0/24 \n
you should enter: 10.0.,10.1.,1.1.1.\n
\n
" 20 70 "10.,192.168.,172.16." 2> $TMP_FILE
NETPREFIXES=`head -n1 $TMP_FILE`

package_install_cmd "wget -h" "wget" "Wget"
package_install_cmd "awk --help"  "gawk" "gawk"
package_install_cmd "sed --help"  "sed"  "sed"

#wget -O /dev/null $GRAYLOGHOST  # It is not an URL
#if [ $? -gt 0 ] ; then
#  msg_fail "Could not access GrayLog on $GRAYLOGHOST!"
#  die 1 "GrayLog could not be accessed. Check the entered URL and try again!"
#else
#  msg_ok "Accessing GrayLog correctly on $GRAYLOGHOST!"
#fi

wget -q -O /dev/null "$HZURL/testing"
if [ $? -gt 0 ] ; then
  msg_fail "Could not access Internet!"
  die 1 "Internet could not be reached! Check the entered information and try again!"
else
  msg_ok "Internet could be reached!"
fi



#Install dependencies
package_install "ssh" "ssh"

for pkg in "php5-cli" "host" ; do
    package_install $pkg $pkg
done
 
cmd_if_n0_info "id hogzilla" \
              "useradd -s '/bin/bash' -m hogzilla" \
              "User hogzilla already exists. Will not try to create it again"
cmd_if_n0_info "ls -la /home/hogzilla/.ssh" \
              "su hogzilla -c 'mkdir /home/hogzilla/.ssh'" \
              "Dir /home/hogzilla/.ssh already exists"
cmd_if_n0_info "cat /home/hogzilla/.ssh/id_rsa" \
              "su hogzilla -c \"ssh-keygen -t rsa -f /home/hogzilla/.ssh/id_rsa -q -N '' \"" \
              "SSH key already exists"
cmd_if_n0_info "cat /home/hogzilla/.ssh/authorized_keys" \
              "su hogzilla -c \"cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys\"" \
              "SSH authorized_keys already exists"
cmd_su "hogzilla" "chmod 0600 ~/.ssh/authorized_keys"

grep "^localhost" /home/hogzilla/.ssh/known_hosts >/dev/null
if [ $? -gt 0 ] ; then
cmd_su "hogzilla" "ssh-keyscan localhost >> ~/.ssh/known_hosts"
fi

grep "^0.0.0.0" /home/hogzilla/.ssh/known_hosts >/dev/null
if [ $? -gt 0 ] ; then
cmd_su "hogzilla" "ssh-keyscan 0.0.0.0 >> ~/.ssh/known_hosts"
fi

cmd_if_n0_info "ls $HADOOPDATA" \
              "mkdir -p $HADOOPDATA" \
              "Directory $HADOOPDATA already exists"

cmd "chown hogzilla. $HADOOPDATA"

SPACE=`df $HADOOPDATA | tail -n1 | awk '{print $4}' | sed 's/\([0-9]*\).*/\1/'`
if [ "$SPACE" -lt 50000000 ] ; then
  msg_warning "You have less than 50GB on $HADOOPDATA . Hogzilla may not run properly. Nevertheless installation will continue."
else
  msg_ok "You have enough space on $HADOOPDATA !"
fi

# JAVA
ps auxw | grep java | grep hogzilla | awk '{print $2}' | xargs kill &>/dev/null

java -XshowSettings:properties -version 2>&1 | grep java.vendor | grep Oracle >/dev/null
if [ $? -gt 0 ] ; then
  sleep 1
#  whiptail --yesno \
#  "You don't have Java/Oracle installed. \n
#Would you like me to install it now? \n
#If NO, you will need to install it later and type \n
#some commands to finish the installation." 20 70
#  
#  if [ $? -gt 0 ] ; then
#    echo ""
#    msg_warning "OK. You will need to install Oracle's Java later!"
#    JAVA=false
#  else
    echo ""
    msg_ok "Ok! I'm going to try the Oracle's Java instalation."
    cmd_if_n0_info "grep '^deb .*webupd8team' /etc/apt/sources.list" \
                  "echo \"deb http://ppa.launchpad.net/webupd8team/java/ubuntu precise main\" >> /etc/apt/sources.list" \
                  "deb WebUpd8Team already in /etc/apt/sources.list"
    cmd_if_n0_info "grep '^deb-src.*webupd8team' /etc/apt/sources.list" \
                  "echo \"deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu precise main\" >> /etc/apt/sources.list" \
                  "deb-src WebUpd8Team already in /etc/apt/sources.list"
    cmd "apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886"
    apt-get update
    apt-get install -y oracle-java8-installer
    apt-get install -y oracle-java8-set-default
    
    java -XshowSettings:properties -version 2>&1 | grep java.vendor | grep Oracle > /dev/null
    if [ $? -eq 0 ] ;then
       JAVA=true
    else
       die 1 "Sorry, I failed! I could not install Java automatically. Try to install it manually"
    fi
# fi
else
  msg "Oracle Java already installed"
fi

sleep 1
# Hadoop, HBase, Spark

msg "I'm going to download Hadoop, HBase and Apache Spark now. It can take some minutes..."

directory_exists "/home/hogzilla/app" || cmd_su "hogzilla" "mkdir /home/hogzilla/app"
directory_exists "/home/hogzilla/bin" || cmd_su "hogzilla" "mkdir /home/hogzilla/bin"
cmd_su "hogzilla" "wget -c -O /home/hogzilla/app/hadoop-$HADOOP_VERSION.tar.gz 'http://www.us.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz'"
file_exists "/home/hogzilla/app/hadoop-$HADOOP_VERSION.tar.gz" || die 1 "Could not download hadoop-$HADOOP_VERSION.tar.gz"
cmd_su "hogzilla" "tar xzf /home/hogzilla/app/hadoop-$HADOOP_VERSION.tar.gz -C /home/hogzilla/"
directory_exists "/home/hogzilla/hadoop" && cmd "rm -rf /home/hogzilla/hadoop"
directory_exists "/home/hogzilla/hadoop-$HADOOP_VERSION" && cmd_su "hogzilla" "mv /home/hogzilla/hadoop-$HADOOP_VERSION /home/hogzilla/hadoop"

cmd_su "hogzilla" "wget -c -O /home/hogzilla/app/hbase-$HBASE_VERSION-bin.tar.gz 'http://www.us.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz'"
file_exists "/home/hogzilla/app/hbase-$HBASE_VERSION-bin.tar.gz" || die 1 "Coult not download hbase-$HBASE_VERSION-bin.tar.gz"
cmd_su "hogzilla" "tar xzf /home/hogzilla/app/hbase-$HBASE_VERSION-bin.tar.gz -C /home/hogzilla/"
directory_exists "/home/hogzilla/hbase" && cmd "rm -rf /home/hogzilla/hbase"
directory_exists "/home/hogzilla/hbase-$HBASE_VERSION" && cmd_su "hogzilla" "mv /home/hogzilla/hbase-$HBASE_VERSION /home/hogzilla/hbase"

cmd_su "hogzilla" "wget -c -O /home/hogzilla/app/spark-$SPARK_VERSION-bin-hadoop2.7.tgz 'http://mirror.nbtelecom.com.br/apache/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz'"
file_exists "/home/hogzilla/app/spark-$SPARK_VERSION-bin-hadoop2.7.tgz" || die 1 "Coult not download spark-$SPARK_VERSION-bin-hadoop2.7.tgz"
cmd_su "hogzilla" "tar xzf /home/hogzilla/app/spark-$SPARK_VERSION-bin-hadoop2.7.tgz -C /home/hogzilla/"
cmd "chown -R hogzilla. /home/hogzilla/spark-$SPARK_VERSION-bin-hadoop2.7"
directory_exists "/home/hogzilla/spark" && cmd "rm -rf /home/hogzilla/spark"
directory_exists "/home/hogzilla/spark-$SPARK_VERSION-bin-hadoop2.7" && cmd_su "hogzilla" "mv /home/hogzilla/spark-$SPARK_VERSION-bin-hadoop2.7 /home/hogzilla/spark"

grep "HADOOP_CONF_DIR" /home/hogzilla/.bashrc > /dev/null
if [ $? -gt 0 ] ; then
  cmd_su "hogzilla" "echo 'export HADOOP_HOME=/home/hogzilla/hadoop'                    >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HBASE_HOME=/home/hogzilla/hbase'                      >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export SPARK_HOME=/home/hogzilla/spark'                      >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_MAPRED_HOME=\$HADOOP_HOME'                      >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_COMMON_HOME=\$HADOOP_HOME'                      >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_HDFS_HOME=\$HADOOP_HOME'                        >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export YARN_HOME=\$HADOOP_HOME'                               >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native' >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin'        >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_INSTALL=\$HADOOP_HOME'                          >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_OPTS=\"-Djava.library.path=\$HADOOP_HOME/lib\"' >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop'              >> ~/.bashrc"
  cmd_su "hogzilla" "echo 'export CLASSPATH=\$CLASSPATH:/home/hogzilla/hbase/lib/*'      >> ~/.bashrc"
else
  msg_info "~/.bashrc already changed."
fi

export HADOOP_HOME=/home/hogzilla/hadoop
export HBASE_HOME=/home/hogzilla/hbase
export SPARK_HOME=/home/hogzilla/spark

cmd "grep JAVA_HOME /etc/profile.d/jdk.sh >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh"
cmd "grep JAVA_HOME /etc/profile.d/jdk.sh >> $HBASE_HOME/conf/hbase-env.sh"

grep "fs.default.name" $HADOOP_HOME/etc/hadoop/core-site.xml > /dev/null
if [ $? -gt 0 ] ; then
   sed -i.original $HADOOP_HOME/etc/hadoop/core-site.xml \
   -e 's#</configuration>#\
      <property>\
         <name>fs.default.name</name>\
         <value>hdfs://localhost:9000</value>\
      </property>\
</configuration>#'
else
  msg_info "$HADOOP_HOME/etc/hadoop/core-site.xml already changed."
fi

grep "dfs.name.dir" $HADOOP_HOME/etc/hadoop/hdfs-site.xml > /dev/null
if [ $? -gt 0 ] ; then
   sed -i.original $HADOOP_HOME/etc/hadoop/hdfs-site.xml \
   -e "s#</configuration>#\
   <property>\
      <name>dfs.replication</name >\
      <value>1</value>\
   </property>\
   <property>\
      <name>dfs.name.dir</name>\
      <value>file:///$HADOOPDATA/hdfs/namenode</value>\
   </property>\
   <property>\
      <name>dfs.data.dir</name>\
      <value>file:///$HADOOPDATA/hdfs/datanode</value>\
   </property>\
</configuration>#"
else
  msg_info "$HADOOP_HOME/etc/hadoop/hdfs-site.xml already changed."
fi

grep "yarn.nodemanager.aux-services" $HADOOP_HOME/etc/hadoop/yarn-site.xml > /dev/null
if [ $? -gt 0 ] ; then
  sed -i.original $HADOOP_HOME/etc/hadoop/yarn-site.xml \
  -e 's#</configuration>#\
   <property>\
      <name>yarn.nodemanager.aux-services</name>\
      <value>mapreduce_shuffle</value>\
   </property>\
</configuration>#'
else
  msg_info "$HADOOP_HOME/etc/hadoop/yarn-site.xml already changed."
fi

#grep "mapreduce.framework.name" $HADOOP_HOME/etc/hadoop/mapred-site.xml > /dev/null
#if [ $? -gt 0 ] ; then
#   sed -i.original $HADOOP_HOME/etc/hadoop/mapred-site.xml \
#   -e 's#</configuration>#\
#   <property>\
#      <name>mapreduce.framework.name</name>\
#      <value>yarn</value>\
#   </property>\
#</configuration>#'
#else
#  msg_info "$HADOOP_HOME/etc/hadoop/mapred-site.xml already changed."
#fi


grep "zookeeper.znode.rootserver" $HBASE_HOME/conf/hbase-site.xml > /dev/null
if [ $? -gt 0 ] ; then
   sed -i.original $HBASE_HOME/conf/hbase-site.xml \
   -e 's#</configuration>#\
   <property>\
       <name>zookeeper.znode.rootserver</name>\
       <value>localhost</value>\
   </property>\
   <property>\
       <name>hbase.cluster.distributed</name>\
       <value>true</value>\
   </property>\
   <property>\
       <name>hbase.rootdir</name>\
       <value>hdfs://localhost:9000/hbase</value>\
   </property>\
   <property>\
       <!-- <name>hbase.regionserver.lease.period</name> -->\
       <name>hbase.client.scanner.timeout.period</name>\
       <value>900000</value> <!-- 900 000, 15 minutes -->\
   </property>\
   <property>\
       <name>hbase.rpc.timeout</name>\
       <value>900000</value> <!-- 15 minutes -->\
   </property>\
   <property>\
       <name>hbase.thrift.connection.max-idletime</name>\
       <value>1800000</value>\
   </property>\
</configuration>#'
else
  msg_info "$HBASE_HOME/conf/hbase-site.xml already changed."
fi

cmd_if_n0_info "grep \"^SPARK_DRIVER_MEMORY\" $SPARK_HOME/conf/spark-env.sh" \
              "echo 'SPARK_DRIVER_MEMORY=1G' >> $SPARK_HOME/conf/spark-env.sh" \
              "$SPARK_HOME/conf/spark-env.sh already changed."

# Format Hadoop
cmd_if_n0_info "ls $HADOOPDATA/hdfs/namenode/" \
              "su hogzilla -c '/home/hogzilla/hadoop/bin/hdfs namenode -format'" \
              "Hadoop Data already formated"

ps auxw | grep java | grep hogzilla | awk '{print $2}' | xargs kill -9 &>/dev/null
cmd "sleep 1"
cmd_su "hogzilla" "$HBASE_HOME/bin/hbase-daemon.sh stop thrift"
cmd_su "hogzilla" "$HBASE_HOME/bin/stop-hbase.sh"
cmd_su "hogzilla" "$HADOOP_HOME/sbin/stop-dfs.sh"
cmd_su "hogzilla" "$HADOOP_HOME/sbin/stop-yarn.sh"
msg_info "Waiting 3 secs..."
cmd "sleep 3"
cmd_su "hogzilla" "$HADOOP_HOME/sbin/start-dfs.sh"
cmd_su "hogzilla" "$HADOOP_HOME/sbin/start-yarn.sh"
cmd_su "hogzilla" "$HBASE_HOME/bin/start-hbase.sh"
cmd_su "hogzilla" "$HBASE_HOME/bin/hbase-daemon.sh start thrift"
msg_info "Waiting 3 secs..."
cmd "sleep 3"


msg_info "Will insert data into HBase now!"
cmd "echo \"create 'hogzilla_flows','flow','event'\"                      > /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_sflows','flow'\"                             >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_events','event'\"                            >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_sensor','sensor'\"                           >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_signatures','signature'\"                    >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_mynets','net'\"                              >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_reputation','rep'\"                          >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_histograms','info','values'\"                >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_clusters','info'\"                           >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_cluster_members','info','member','cluster'\" >> /tmp/.hogzilla_hbase_script"
cmd "echo \"create 'hogzilla_inventory','info'\"                          >> /tmp/.hogzilla_hbase_script"

for net in `echo $NETPREFIXES | sed 's/,/ /g'` ; do 
   cmd "echo \"put 'hogzilla_mynets', '$net', 'net:description', 'Desc $net'\"   >> /tmp/.hogzilla_hbase_script"
   cmd "echo \"put 'hogzilla_mynets', '$net', 'net:prefix', '$net'\"             >> /tmp/.hogzilla_hbase_script"
done
cmd "echo \"exit\"                          >> /tmp/.hogzilla_hbase_script"

cmd "chown hogzilla. /tmp/.hogzilla_hbase_script"
cmd_su "hogzilla" "$HBASE_HOME/bin/hbase shell /tmp/.hogzilla_hbase_script"
cmd "rm -f /tmp/.hogzilla_hbase_script"

# Thrift
cmd "apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 379CE192D401AB61"


cmd "wget -q -O/etc/apt/trusted.gpg.d/altern-deb-jessie-stable.gpg https://altern-deb.com/debian/package-signing-key@altern-deb.com.gpg"

cmd_if_n0_info "grep altern-deb /etc/apt/sources.list" \
               "echo 'deb http://altern-deb.com/debian/  jessie  main' >> /etc/apt/sources.list" \
               "altern-deb.com already in /etc/apt/sources.list"

#cmd_if_n0_info "grep dl.bintray.com /etc/apt/sources.list" \
#               "echo 'deb http://dl.bintray.com/apache/thrift/debian/ 0.9.3 main' >> /etc/apt/sources.list" \
#               "bintray.com already in /etc/apt/sources.list"

apt-get update

for pkg in "php5-thrift" "gcc" "automake" "autoconf" "make" "libthrift0"; do
    package_install "$pkg" "$pkg"
done


# Composer... Not needed.
#cmd_su "hogzilla" "wget -c -O /home/hogzilla/bin/composer 'https://getcomposer.org/download/1.2.1/composer.phar'"
#file_exists "/home/hogzilla/bin/composer" || die 1 "Coult not download Composer"
#cmd_su "hogzilla" "chmod 755 /home/hogzilla/bin/composer"

# Hogzilla
cmd_su "hogzilla" "wget -c -O /home/hogzilla/Hogzilla.jar '$HZURL/downloads/Hogzilla-v0.6-latest-beta.jar'"
file_exists "/home/hogzilla/Hogzilla.jar" || die 1 "Coult not download Hogzilla-v0.6-latest-beta.jar"

# Pigtail
cmd_su "hogzilla" "wget -c -O /home/hogzilla/app/pigtail-v1.1-latest.tar.gz '$HZURL/downloads/pigtail-v1.1-latest.tar.gz'"
file_exists "/home/hogzilla/app/pigtail-v1.1-latest.tar.gz" || die 1 "Coult not download pigtail-v1.1-latest.tar.gz"
cmd_su "hogzilla" "tar xzf /home/hogzilla/app/pigtail-v1.1-latest.tar.gz -C /home/hogzilla/"

directory_exists "/usr/share/php/Thrift/Packages/" || cmd "mkdir /usr/share/php/Thrift/Packages/"
directory_exists "/home/hogzilla/pigtail/gen-php/Hbase/" && cmd "cp -a /home/hogzilla/pigtail/gen-php/Hbase/ /usr/share/php/Thrift/Packages/"

file_exists "/home/hogzilla/pigtail/pigtail.php" && sed -i.original /home/hogzilla/pigtail/pigtail.php -e 's#grayloghost#$GRAYLOGHOST#'

# Hogzilla utils
cmd_su "hogzilla" "wget -c -O /home/hogzilla/app/hz-utils-v1.0-latest.tar.gz '$HZURL/downloads/hz-utils-v1.0-latest.tar.gz'"
file_exists "/home/hogzilla/app/hz-utils-v1.0-latest.tar.gz" || die 1 "Coult not download hz-utils-v1.0-latest.tar.gz"
cmd_su "hogzilla" "tar xzf /home/hogzilla/app/hz-utils-v1.0-latest.tar.gz -C /home/hogzilla/"
directory_exists "/home/hogzilla/hz-utils/" && cmd_su "hogzilla" "mv -f /home/hogzilla/hz-utils/* /home/hogzilla/bin/"

cmd_su "hogzilla" "sed -i.original /home/hogzilla/bin/start-hogzilla.sh -e \"s#HBASE_VERSION=.1.1.5.#HBASE_VERSION='$HBASE_VERSION'#\""

# SFLOWTOOL
cmd_su "hogzilla" "wget --no-check-certificate -c -O /home/hogzilla/app/sflowtool-$SFLOWTOOL_VERSION.tar.gz 'https://github.com/sflow/sflowtool/releases/download/v$SFLOWTOOL_VERSION/sflowtool-$SFLOWTOOL_VERSION.tar.gz'"
file_exists "/home/hogzilla/app/sflowtool-$SFLOWTOOL_VERSION.tar.gz" || die 1 "Coult not download sflowtool-$SFLOWTOOL_VERSION.tar.gz"
directory_exists "/home/hogzilla/sflowtool-$SFLOWTOOL_VERSION" && cmd "rm -rf /home/hogzilla/sflowtool-$SFLOWTOOL_VERSION"
cmd_su "hogzilla" "tar xzf /home/hogzilla/app/sflowtool-$SFLOWTOOL_VERSION.tar.gz -C /home/hogzilla/"
su hogzilla -c "cd /home/hogzilla/sflowtool-$SFLOWTOOL_VERSION ; ./configure"
su hogzilla -c "cd /home/hogzilla/sflowtool-$SFLOWTOOL_VERSION ; make"
cd /home/hogzilla/sflowtool-$SFLOWTOOL_VERSION ; make install

msg_notice "I'm going to start sFlow collector, Hogzilla processing, DBUpdates and PigTail."
su - hogzilla -c "/home/hogzilla/bin/start-pigtail.sh"
su - hogzilla -c "/home/hogzilla/bin/start-hogzilla.sh"
su - hogzilla -c "/home/hogzilla/bin/start-sflow2hz.sh"
su - hogzilla -c "/home/hogzilla/bin/start-dbupdates.sh"

cmd_if_n0_info "grep start-all.sh /etc/rc.local" \
               "sed -i.original /etc/rc.local -e 's#exit 0#/home/hogzilla/bin/start-all.sh \&\nexit 0#'" \
              "/etc/rc.local already updated"

whiptail --title "YOU WILL NEED" --msgbox \
"
Now, to make everything run, you will need:\n
\n
1) Create an input at your GrayLog*\n
2) Configure your router to send SFlows to Hogzilla's IP*\n
3) Wait some time for data collection and processing\n
4) You can also send sFlows directly to GrayLog. It can be used to incident analysis.\n
\n
*Know how you can do these at $HZURL
\n
\n
" 20 70 

whiptail --title "INSTALLATION FINISHED" --msgbox \
"
Hogzilla IDS now is installed.\n
Be patient, it will need around 7 days to learn and execute all procedures.\n\n
The Hogzilla IDS' community needs your help. If you have any trouble, share with us!\n
See how at $HZURL \n
" 20 70 
echo ""
msg_ok "Installation finished! Hogzilla IDS will need around 7 days to learn and execute all procedures."
