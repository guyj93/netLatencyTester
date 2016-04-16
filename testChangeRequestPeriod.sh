rpArray=(10ms 3ms 1ms 300us 100us 30us 10us 3us 1us 300ns 100ns 30ns 10ns 3ns 1ns 0ns)
scriptPath=$(cd $(dirname "${BASH_SOURCE[0]}");pwd;)
for rp in ${rpArray[@]}; do
  echo -e "${rp}\t\c"
  $scriptPath/netLatencyTester -rp $rp $*
done
