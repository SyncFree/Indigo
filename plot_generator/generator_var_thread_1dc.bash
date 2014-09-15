DIR=$1
OUT_DIR=$2
FILES=$1results-indigo-k*-r1-t*-v9999999-uniform*/TPSL/*US-EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -k $FILES > TPS_K-r1-indigo-EUROPE.dat

FILES=$1results-weak-k*-r1-t*-v9999999-uniform*/TPSL/*US_EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -k $FILES > TPS_K-r1-weak-EUROPE.dat

gnuplot -e "iuse='TPS_K-r1-indigo-EUROPE.dat'" -e "wuse='TPS_K-r1-weak-EUROPE.dat'" \
			plot_generator/TPS_KEY.gnuplot > $OUT_DIR/TPS_K-r1.ps
