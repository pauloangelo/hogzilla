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

echo ""
if [ $? -gt 0 ] ; then
  msg_warning "OK. We are finishing now!"
else
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
for pkg in "php5-dev" "php5-cli" "phpunit"  ; do
    package_installed $pkg $pkg
done
 
#Create users

#Gen SSH keys


# Install java
# Hadoop
# Install
# Check

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
