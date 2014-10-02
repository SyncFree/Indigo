RES_DIR=$1
DATA_DIR=$2
OUT_DIR=$3

#grep -hv 'START' indigo_results-scalability-new/results-indigo-k1000-r3-t10-v2999999-uniform/TPSL/*.dat | awk -F '\t' '{print $1" "$2'}


DIR_ARRAY=(`echo $RES_DIR"/results_tournament*-c-*-t*"`)

for dir in "${DIR_ARRAY[@]}"
do
	:
	rm $dir/TPSL/tournament_results_ALL.dat
	grep -hv 'START' $dir/TPSL/*.dat | awk -F '\t' '{print $1"\t"$2"\t"$3'} >> $dir/TPSL/tournament_results_ALL.dat
done

FILES=$RES_DIR/results_tournament-indigo-c-indigo*-t*/TPSL/*ALL*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsa -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/TPS_T-T-indigo-ALL.dat

FILES=$RES_DIR/results_tournament-weak-c-indigo*-t*/TPSL/*ALL* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsa -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/TPS_T-T-weak-ALL.dat


FILES=$RES_DIR/results_tournament_redblue-c-global-indigo*-t*/TPSL/*ALL* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsa -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/TPS_T-T-redblue-ALL.dat

rm tmp_unsorted

gnuplot -e "iall='$DATA_DIR/TPS_T-T-indigo-ALL.dat'"\
		-e "wall='$DATA_DIR/TPS_T-T-weak-ALL.dat'"\
		-e "sall='$DATA_DIR/TPS_T-T-redblue-ALL.dat'"\
			plot_generator/TPS_Threads_Multi_ALL.gnuplot > $OUT_DIR/TPS_T-k1000-r3-ALL-TOURNAMENT.ps


