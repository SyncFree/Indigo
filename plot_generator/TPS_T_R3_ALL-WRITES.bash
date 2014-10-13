RES_DIR=$1
DATA_DIR=$2
OUT_DIR=$3

#grep -hv 'START' indigo_results-scalability-new/results-indigo-k1000-r3-t10-v2999999-uniform/TPSL/*.dat | awk -F '\t' '{print $1" "$2'}


DIR_ARRAY=(`echo $RES_DIR"/*results-*R*W*-t*"`)

for dir in "${DIR_ARRAY[@]}"
do
	:
	rm $dir/TPSL/micro_benchmark_results_ALL.dat
	grep -hv 'START' $dir/TPSL/*.dat | awk -F '\t' '{print $1"\t"$2"\t"$3'} >> $dir/TPSL/micro_benchmark_results_ALL.dat
done

FILES=$RES_DIR/results-indigo-R5-W1-k1000-r3-t*-v2999999-uniform*/TPSL/*ALL*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsa -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/TPS_T-R5-W1-k1000-r3-v2999999-indigo-ALL.dat

FILES=$RES_DIR/results-indigo-R5-W2-k1000-r3-t*-v2999999-uniform*/TPSL/*ALL*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsa -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/TPS_T-R5-W2-k1000-r3-v2999999-indigo-ALL.dat

FILES=$RES_DIR/results-indigo-R5-W3-k1000-r3-t*-v2999999-uniform*/TPSL/*ALL*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsa -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/TPS_T-R5-W3-k1000-r3-v2999999-indigo-ALL.dat

gnuplot -e "iallr5w1='$DATA_DIR/TPS_T-R5-W1-k1000-r3-v2999999-indigo-ALL.dat'"\
		-e "iallr5w2='$DATA_DIR/TPS_T-R5-W2-k1000-r3-v2999999-indigo-ALL.dat'"\
		-e "iallr5w3='$DATA_DIR/TPS_T-R5-W3-k1000-r3-v2999999-indigo-ALL.dat'"\
			plot_generator/TPS_Threads_Multi_ALL-WRITES.gnuplot > $OUT_DIR/TPS_T-R5-W-k1000-r3-ALL.ps


