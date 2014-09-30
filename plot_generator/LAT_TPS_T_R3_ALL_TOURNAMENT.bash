RES_DIR=$1
DATA_DIR=$2
OUT_DIR=$3


DIR_ARRAY=(`echo $RES_DIR"/results_tournament-*-c-*-t*"`)

for dir in "${DIR_ARRAY[@]}"
do
	:
	rm $dir/tournament_results_ALL.log_tmp
	rm $dir/tournament_results_ALL.log
	
	for f in $dir/*.log
	do
		:
			awk -F '\t' '{if (NR==13) START=$2} {if (length(START) != 0) print $2-START" "$4}' $f >> $dir/tournament_results_ALL.log_tmp 
	done
	sort -n -t "	" $dir/tournament_results_ALL.log_tmp > $dir/tournament_results_ALL.log
	echo "Processing "$dir
	cat $dir/tournament_results_ALL.log | java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tpsl > $dir/TPSL/tournament_results_ALL.dat
done

FILES=$RES_DIR/results_tournament-indigo-c-indigo*-t*/TPSL/*ALL*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-T-indigo-ALL.dat

FILES=$RES_DIR/results_tournament-weak-c-indigo*-t*/TPSL/*ALL* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-T-weak-ALL.dat

FILES=$RES_DIR/results_tournament-indigo-c-global-indigo*-t*/TPSL/*ALL* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-T-strong-ALL.dat

rm tmp_unsorted

gnuplot -e "iall='$DATA_DIR/LAT_TPS_T-T-indigo-ALL.dat'"\
		-e "wall='$DATA_DIR/LAT_TPS_T-T-weak-ALL.dat'"\
		-e "sall='$DATA_DIR/LAT_TPS_T-T-strong-ALL.dat'"\
			plot_generator/LAT_TPS_Multi_ALL.gnuplot > $OUT_DIR/LAT_TPS_T-TOURNAMENT-l100-r3-ALL.ps



