rqsArray=(16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152)
for rqs in ${rqsArray[@]}; do
  echo -e "${rqs}\t\c"
  ./netLatencyTester -rqs $rqs $*
done
