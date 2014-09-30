#FULL_DIR
RES_DIR=$1
OUT_DIR=$2
FILENAME=$3
sed '1,100d'  $RES_DIR/micro_benchmark_results_US-EAST.log | awk '{print $2" "$4" "$7}' > tmp0use & \
#sed '1,100d'  $RES_DIR/remote_indigo_results_US-EAST.log | awk '{print $2" "$7}' > tmp1use & \
sed '1,100d'  $RES_DIR/micro_benchmark_results_US-WEST.log | awk '{print $2" "$4" "$5}' > tmp0usw & \
#sed '1,100d'  $RES_DIR/remote_indigo_results_US-WEST.log | awk '{print $2" "$7}' > tmp1usw & \
sed '1,100d'  $RES_DIR/micro_benchmark_results_EUROPE.log | awk '{print $2" "$4" "$5}' > tmp0eur & \
#sed '1,100d'  $RES_DIR/remote_indigo_results_EUROPE.log | awk '{print $2" "$7}' > tmp1eur & \
	gnuplot  -e "iuse='tmp0use' ; iusw='tmp0usw' ; ieur='tmp0eur'" plot_generator/plot_over_time.gnuplot > $OUT_DIR/$FILENAME
#	gnuplot  -e "iuse='tmp0use' ; iusw='tmp0usw' ; ieur='tmp0eur' ; ruse='tmp1use' ; rusw='tmp1usw' ; reur='tmp1eur'" plot_generator/plot_over_time.gnuplot > $OUT_DIR/$FILENAME
