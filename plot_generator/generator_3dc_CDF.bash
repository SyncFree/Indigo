# ATTENTION: INPUT DIR CANNOT USE "-" character

RES_DIR=$1
OUT_DIR=$2

#Plot for each data-center individually

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r3-t20-v2999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "wuse='$RES_DIR/results-weak-k1000-r3-t20-v2999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
        -e "iusw='$RES_DIR/results-indigo-k1000-r3-t20-v2999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
	 	-e "wusw='$RES_DIR/results-weak-k1000-r3-t20-v2999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
        -e "ieur='$RES_DIR/results-indigo-k1000-r3-t20-v2999999-uniform/CDF/micro_benchmark_results_EUROPE.dat'" \
	 	-e "weur='$RES_DIR/results-weak-k1000-r3-t20-v2999999-uniform/CDF/micro_benchmark_results_EUROPE.dat'" \
        -e "xmax='100"\
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r3-t20_CDF.ps"

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r3-t60-v2999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
        -e "wuse='$RES_DIR/results-weak-k1000-r3-t60-v2999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
        -e "iusw='$RES_DIR/results-indigo-k1000-r3-t60-v2999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
	 	-e "wusw='$RES_DIR/results-weak-k1000-r3-t60-v2999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
        -e "ieur='$RES_DIR/results-indigo-k1000-r3-t60-v2999999-uniform/CDF/micro_benchmark_results_EUROPE.dat'" \
	 	-e "weur='$RES_DIR/results-weak-k1000-r3-t60-v2999999-uniform/CDF/micro_benchmark_results_EUROPE.dat'" \
        -e "xmax='200"\
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r3-t60_CDF.ps"

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r3-t100-v2999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
        -e "wuse='$RES_DIR/results-weak-k1000-r3-t100-v2999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
        -e "iusw='$RES_DIR/results-indigo-k1000-r3-t100-v2999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
	 	-e "wusw='$RES_DIR/results-weak-k1000-r3-t100-v2999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
        -e "ieur='$RES_DIR/results-indigo-k1000-r3-t100-v2999999-uniform/CDF/micro_benchmark_results_EUROPE.dat'" \
	 	-e "weur='$RES_DIR/results-weak-k1000-r3-t100-v2999999-uniform/CDF/micro_benchmark_results_EUROPE.dat'" \
        -e "xmax='400"\
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r3-t100_CDF.ps"
