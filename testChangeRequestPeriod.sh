rpArray=(10ms 3ms 1ms 300us 100us 30us 10us 3us 1us 300ns 100ns 30ns 10ns 3ns 1ns 0ns)
for rp in ${rpArray[@]}; do
  echo -e "${rp}\t\c"
  ./netLatencyTester -rp $rp $*
done
