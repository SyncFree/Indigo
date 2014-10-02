RES_DIR=$1
DATA_DIR=$2
OUT_DIR=$3


DIR_ARRAY=(`echo $RES_DIR"*results-*-*-t*"`)

for dir in "${DIR_ARRAY[@]}"
do
	:
	rm $dir/micro_benchmark_results_ALL.log_tmp
	rm $dir/micro_benchmark_results_ALL.log
	
	for f in $dir/*.log
	do
		:
			awk -F '\t' '{if (NR==16) START=$2} {if (length(START) != 0) print $2-START" "$4}' $f >> $dir/micro_benchmark_results_ALL.log_tmp 
	done
	sort -n -t "	" $dir/micro_benchmark_results_ALL.log_tmp > $dir/micro_benchmark_results_ALL.log
	echo "Processing "$dir
	cat $dir/micro_benchmark_results_ALL.log | java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsl > $dir/TPSL/micro_benchmark_results_ALL.dat
done

FILES=$RES_DIR/results-indigo-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-ALL.dat

FILES=$RES_DIR/results-weak-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-ALL.dat

FILES=$RES_DIR/results-redblue-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-redblue-ALL.dat

FILES=$RES_DIR/new-results-strong-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-strong-ALL.dat

gnuplot -e "iall='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-ALL.dat'"\
		-e "wall='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-ALL.dat'"\
		-e "rball='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-redblue-ALL.dat'"\
		-e "sall='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-strong-ALL.dat'"\
			plot_generator/LAT_TPS_Multi_ALL.gnuplot > $OUT_DIR/LAT_TPS_T-k1000-r3-ALL.ps

FILES=$RES_DIR/results-indigo-R5-W1-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-indigo-ALL.dat

FILES=$RES_DIR/results-weak-R5-W1-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-weak-ALL.dat

FILES=$RES_DIR/results-redblue-R5-W1-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-redblue-ALL.dat

FILES=$RES_DIR/new-results-strong-R5-W1-k1000-r3-t*-v2999999-uniform/TPSL/*ALL.dat
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-strong-ALL.dat


rm tmp_unsorted

gnuplot -e "iall='$DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-indigo-ALL.dat'"\
		-e "wall='$DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-weak-ALL.dat'"\
		-e "rball='$DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-redblue-ALL.dat'"\
		-e "sall='$DATA_DIR/LAT_TPS_T-R5-W1-k1000-r3-v2999999-strong-ALL.dat'"\
			plot_generator/LAT_TPS_Multi_ALL.gnuplot > $OUT_DIR/LAT_TPS_T-R5-W1-k1000-r3-ALL.ps



