#!/bin/bash

## Create repositories IPs list
#
# Usage: ./getReposList.sh <system>
#      Ex: ./getReposList.sh windows


if test -z $1 
then
   echo "Usage: ./getReposList.sh <system>"
   echo "System: windows, linux, freebsd, apple, or android"
   exit 1
fi

#### URLs utilizadas pelos sistemas


# Windows
URLs_windows="windowsupdate.microsoft.com update.microsoft.com windowsupdate.com download.windowsupdate.com download.microsoft.com download.windowsupdate.com ntservicepack.microsoft.com time.windows.com javadl-esd.sun.com fpdownload.adobe.com cache.pack.google.com aus2.mozilla.org aus3.mozilla.org aus4.mozilla.org avast.com files.avast.com"

# Linux
URLs_linux="security.ubuntu.com security.debian.org mirrorlist.centos.org 0.rhel.pool.ntp.org 1.rhel.pool.ntp.org 2.rhel.pool.ntp.org ntp.ubuntu.com linux.dropbox.com"

#Android
URLs_android="play.google.com android.clients.google.com"

# Apple
URLs_ios="phobos.apple.com deimos3.apple.com albert.apple.com gs.apple.com itunes.apple.com ax.itunes.apple.com"

# FreeBSD
URLs_bsd="ftp.freebsd.org"

function IP6_to_long()
{
        INPUT="$(tr 'A-F' 'a-f' <<< "$@")"
        O=""
        while [ "$O" != "$INPUT" ]; do
                O="$INPUT"
                # fill all words with zeroes
                INPUT="$( sed  's|:\([0-9a-f]\{3\}\):|:0\1:|g' <<< "$INPUT" )"
                INPUT="$( sed  's|:\([0-9a-f]\{3\}\)$|:0\1|g'  <<< "$INPUT")"
                INPUT="$( sed  's|^\([0-9a-f]\{3\}\):|0\1:|g'  <<< "$INPUT" )"
                INPUT="$( sed  's|:\([0-9a-f]\{2\}\):|:00\1:|g' <<< "$INPUT")"
                INPUT="$( sed  's|:\([0-9a-f]\{2\}\)$|:00\1|g'  <<< "$INPUT")"
                INPUT="$( sed  's|^\([0-9a-f]\{2\}\):|00\1:|g'  <<< "$INPUT")"
                INPUT="$( sed  's|:\([0-9a-f]\):|:000\1:|g'  <<< "$INPUT")"
                INPUT="$( sed  's|:\([0-9a-f]\)$|:000\1|g'   <<< "$INPUT")"
                INPUT="$( sed  's|^\([0-9a-f]\):|000\1:|g'   <<< "$INPUT")"
        done
        # now expand the ::
        ZEROES=""
        grep -qs "::" <<< "$INPUT"
        if [ "$?" -eq 0 ]; then
          GRPS="$(sed  's|[0-9a-f]||g' <<< "$INPUT" | wc -m)"
          ((GRPS--)) # carriage return
          ((MISSING=8-GRPS))
          for ((i=0;i<$MISSING;i++)); do
                  ZEROES="$ZEROES:0000"
          done
        # be careful where to place the :
         INPUT="$( sed  's|\(.\)::\(.\)|\1'$ZEROES':\2|g'   <<< "$INPUT")"
         INPUT="$( sed  's|\(.\)::$|\1'$ZEROES':0000|g'   <<< "$INPUT")"
         INPUT="$( sed  's|^::\(.\)|'$ZEROES':0000:\1|g;s|^:||g'   <<< "$INPUT")"
        
        fi

        # an expanded address has 39 chars + CR
        if [ $(echo $INPUT | wc -m) != 40 ]; then
         echo "invalid IPv6 Address"
        fi

        # echo the fully expanded version of the address
        echo $INPUT 
}

function GET_IP_WINDOWS()
 {
 IPs_win=""
        for url in $(echo $URLs_windows)
        do
                IPs_win=`echo $IPs_win  & host $url | sed 's/IPv6 //g'| cut -d " " -f4 | grep  "^[0-9]"`
        done
        echo $IPs_win | sed 's/ /\n/g' | sort -u | grep -v ":"
        IPv6=`echo $IPs_win | sed 's/ /\n/g' | sort -u | grep  ":"`
        for ip6 in $(echo $IPv6)
        do
           IP6_to_long $ip6
        done
 }
function GET_IP_LINUX()
{
IPs_lnx=""
 for url in $(echo $URLs_linux)
        do
                IPs_lnx=`echo $IPs_lnx  & host $url | sed 's/IPv6 //g'| cut -d " " -f4 | grep  "^[0-9]"`
        done
        echo $IPs_lnx | sed 's/ /\n/g' |sort -u | grep -v ":"
        IPv6=`echo $IPs_lnx | sed 's/ /\n/g' | sort -u | grep  ":"`
        for ip6 in $(echo $IPv6)
        do
           IP6_to_long $ip6
        done

}

function GET_IP_ANDROID()
{
IPs_andr=""
 for url in $(echo $URLs_android)
        do
                IPs_andr=`echo $IPs_andr  & host $url | sed 's/IPv6 //g'| cut -d " " -f4 | grep  "^[0-9]"`
        done
        echo $IPs_andr | sed 's/ /\n/g' |sort -u | grep -v ":"
        IPv6=`echo $IPs_andr | sed 's/ /\n/g' | sort -u | grep  ":"`
        for ip6 in $(echo $IPv6)
        do
           IP6_to_long $ip6
        done

}
function GET_IP_IOS()
{
IPs_ios=""
 for url in $(echo $URLs_ios)
        do
                IPs_ios=`echo $IPs_ios  & host $url | sed 's/IPv6 //g'| cut -d " " -f4 | grep  "^[0-9]"`
        done
        echo $IPs_ios | sed 's/ /\n/g' |sort -u | grep -v ":"
        IPv6=`echo $IPs_ios | sed 's/ /\n/g' | sort -u | grep  ":"`
        for ip6 in $(echo $IPv6)
        do
           IP6_to_long $ip6
        done

}


function GET_IP_FREEBSD()
{
IPs_bsd=""
 for url in $(echo $URLs_bsd)
        do
                IPs_bsd=`echo $IPs_bsd  & host $url | sed 's/IPv6 //g'| cut -d " " -f4 | grep  "^[0-9]"`
        done
        echo $IPs_bsd | sed 's/ /\n/g' |sort -u | grep -v ":"
        IPv6=`echo $IPs_bsd | sed 's/ /\n/g' | sort -u | grep  ":"`
        for ip6 in $(echo $IPv6)
        do
           IP6_to_long $ip6
        done
}


case $1 in
        windows) GET_IP_WINDOWS;;
        linux)  GET_IP_LINUX;;
        freebsd) GET_IP_FREEBSD;;
        android) GET_IP_ANDROID;;
        apple) GET_IP_IOS;;

        *) echo "Invalid Option"
        ;;
esac
