#FULL_DIR
RES_DIR=$1
OUT_DIR=$2
FILENAME=$3
sed '1,100d'  $RES_DIR/micro_benchmark_results_US-WEST.log | awk '{print $2" "$4" "$7}' > tmp0usw

gnuplot  -e "iusw='tmp0usw'" plot_generator/simple_plot_over_time_1_dc.gnuplot > $OUT_DIR/$FILENAME
