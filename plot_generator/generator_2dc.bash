# ATTENTION: INPUT DIR CANNOT USE "-" character

RES_DIR=$1
OUT_DIR=$2

#Plot for each data-center individually

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r2-t20-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "wuse='$RES_DIR/results-weak-k1000-r2-t20-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r2-t20_CDF_US-EAST.ps"

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r2-t40-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "wuse='$RES_DIR/results-weak-k1000-r2-t40-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r2-t40_CDF_US-EAST.ps"

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r2-t20-v9999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
	 	-e "wuse='$RES_DIR/results-weak-k1000-r2-t20-v9999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r2-t20_CDF_US-WEST.ps"

gnuplot -e "iuse='$RES_DIR/results-indigo-k1000-r2-t40-v9999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
	 	-e "wuse='$RES_DIR/results-weak-k1000-r2-t40-v9999999-uniform/CDF/micro_benchmark_results_US-WEST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > $OUT_DIR"k1000-r2-t40_CDF_US-WEST.ps"

#Plot TPS/#Thread for all data-centers and commulative

FILES=$RES_DIR/results-indigo-k1000-r2-t*-v9999999-uniform*/TPSL/*US-EAST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r2-v9999999-indigo-US-EAST.dat

FILES=$RES_DIR/results-weak-k1000-r2-t*-v9999999-uniform*/TPSL/*US-EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r2-v9999999-weak-US-EAST.dat

FILES=$RES_DIR/results-indigo-k1000-r2-t*-v9999999-uniform*/TPSL/*US-WEST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r2-v9999999-indigo-US-WEST.dat

FILES=$RES_DIR/results-weak-k1000-r2-t*-v9999999-uniform*/TPSL/*US-WEST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r2-v9999999-weak-US-WEST.dat

#rm $RES_DIR/results-*-k1000-r2-t*-v9999999-uniform/*tmp
#rm $RES_DIR/results-*-k1000-r2-t*-v9999999-uniform/*ALL.log
#FILES=$RES_DIR/results-*-k1000-r2-t*-v9999999-uniform/*.log

#for f in ${FILES[@]}
#do
#	dir=`dirname $f`
#	cat $f | grep -v '^-' | grep -v 'OP_NAME' | grep -v '^$' >> $dir/tmp 
#done 

#for f in "$RES_DIR"results-*-k1000-r2-t*-v9999999-uniform/*tmp
#do
#	dir=`dirname $f`
#	cat $f | sort -k 2 > $dir"/micro_benchmark_results_ALL.log"
#	rm $dir/*tmp
#	awk -F '\t' '{print $2" "$4}' $dir/micro_benchmark_results_ALL.log | java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsl $dir/micro_benchmark_results_ALL.log > $dir/TPSL/micro_benchmark_results_ALL.dat
	
#done

#FILES=$RES_DIR/results-indigo-k1000-r2-t*-v9999999-uniform/TPSL/*ALL* 
#java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r2-v9999999-indigo-ALL.dat

#FILES=$RES_DIR/results-weak-k1000-r2-t*-v9999999-uniform/*ALL* 
#java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r2-v9999999-weak-ALL.dat


gnuplot -e "iuse='TPS_T-k1000-r2-v9999999-indigo-US-EAST.dat'" -e "wuse='TPS_T-k1000-r2-v9999999-weak-US-EAST.dat'" \
		-e "iusw='TPS_T-k1000-r2-v9999999-indigo-US-WEST.dat'" -e "wusw='TPS_T-k1000-r2-v9999999-weak-US-WEST.dat'" \
			plot_generator/TPS_Threads_Multi.gnuplot > $OUT_DIR/TPS_T-k1000-r2.ps


