function ltrim(s) { sub(/^[ \t\r\n]+/, "", s); return s }
function rtrim(s) { sub(/[ \t\r\n]+$/, "", s); return s }
function trim(s) { return rtrim(ltrim(s)); }

$1 ~ /^Username/ {

  username =  $2;
  getline;
  tag=$2
  content = gensub(/(.*):(.*)/,"\\2","g",$0);
  getline; getline; getline; 
  location = gensub(/.*Location: (.*), UA:.*/,"\\1","g",$0);
  UA = gensub(/.*UA: (.*), AuthMethod:.*/,"\\1","g",$0);
  DATE = gensub(/.*TIME: (.*), .*/,"\\1","g",$0);                                     
  IP = gensub(/^(.*) =.*/,"\\1","g",$0);                                              
  IP = gensub(/(.*)\(.*\)/,"\\1","g",IP);


  if(tag ~ /Cities/){
     type="LOCATION"
  } else
  if(tag ~ /UserAgents/){
     type="UA"
  }
     print type"|"username"|"trim(DATE)"|"trim(IP)"|"trim(location)"|"trim(UA);       


}
