RES_DIR=$1
DATA_DIR=$2
OUT_DIR=$3


FILES=$RES_DIR/results-indigo-k1000-r3-t*-v2999999-uniform*/TPSL/*US-EAST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-US-EAST.dat

FILES=$RES_DIR/results-weak-k1000-r3-t*-v2999999-uniform*/TPSL/*US-EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-US-EAST.dat

FILES=$RES_DIR/results-indigo-k1000-r3-t*-v2999999-uniform*/TPSL/*US-WEST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-US-WEST.dat

FILES=$RES_DIR/results-weak-k1000-r3-t*-v2999999-uniform*/TPSL/*US-WEST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-US-WEST.dat

FILES=$RES_DIR/results-indigo-k1000-r3-t*-v2999999-uniform*/TPSL/*EUROPE*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-EUROPE.dat

FILES=$RES_DIR/results-weak-k1000-r3-t*-v2999999-uniform*/TPSL/*EUROPE*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -latTPS -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > $DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-EUROPE.dat

rm tmp_unsorted

gnuplot -e "iuse='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-US-EAST.dat'"\
        -e "wuse='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-US-EAST.dat'"\
		-e "iusw='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-US-WEST.dat'"\
        -e "wusw='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-US-WEST.dat'" \
      	-e "ieur='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-indigo-EUROPE.dat'"\
        -e "weur='$DATA_DIR/LAT_TPS_T-k1000-r3-v2999999-weak-EUROPE.dat'" \
			plot_generator/LAT_TPS_Multi.gnuplot > $OUT_DIR/LAT_TPS_T-k1000-r3.ps



