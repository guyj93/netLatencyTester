rqsArray=(16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152)
scriptPath=$(cd $(dirname "${BASH_SOURCE[0]}");pwd;)
for rqs in ${rqsArray[@]}; do
  echo -e "${rqs}\t\c"
  $scriptPath/netLatencyTester -rqs $rqs $*
done
