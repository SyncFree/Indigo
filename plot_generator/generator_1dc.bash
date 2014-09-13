gnuplot -e "iuse='/Users/balegas/workspace/java/indigo_results/results-indigo-k100-r1-t10-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "wuse='/Users/balegas/workspace/java/indigo_results/results-weak-k100-r1-t10-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > k100-r1-t10-v9999999_CDF.ps

gnuplot -e "iuse='/Users/balegas/workspace/java/indigo_results/results-indigo-k100-r1-t1-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "wuse='/Users/balegas/workspace/java/indigo_results/results-weak-k100-r1-t1-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > k100-r1-t1-v9999999_CDF.ps

gnuplot -e "iuse='/Users/balegas/workspace/java/indigo_results/results-indigo-k1-r1-t1-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "wuse='/Users/balegas/workspace/java/indigo_results/results-weak-k1-r1-t1-v9999999-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > k1-r1-t1-v9999999_CDF.ps

FILES=../indigo_results/results-indigo-k100-r1-t*-v9999999-uniform/TPSL/*US-EAST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k100-r1-v9999999-indigo-US-EAST.dat

FILES=../indigo_results/results-weak-k100-r1-t*-v9999999-uniform/TPSL/*US-EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k100-r1-v9999999-weak-US-EAST.dat

FILES=../indigo_results/results-*-k100-r1-t*-v9999999-uniform/*.log

for f in ${FILES[@]}
do
	dir=`dirname $f`
	cat $f | sort -k 2 | grep -v '^-' | grep -v 'OP_NAME' > $dir"/micro_benchmark_results_ALL.log"
done 

FILES=../indigo_results/results-indigo-k*-r1-t*-v9999999-uniform/*ALL* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > TPS_T-k100-r1-v9999999-indigo-ALL.dat

FILES=../indigo_results/results-weak-k*-r1-t*-v9999999-uniform/*ALL* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > TPS_T-k100-r1-v9999999-weak-ALL.dat

gnuplot -e "iuse='TPS_T-k100-r1-v9999999-indigo-US-EAST.dat'" -e "wuse='TPS_T-k100-r1-v9999999-weak-US-EAST.dat'" \
			plot_generator/TPS_Threads.gnuplot > TPS_T-k100-r1-v9999999.ps

FILES=../indigo_results/results-indigo-k1000-r1-t*-v9999999-uniform/TPSL/*US-EAST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted > TPS_T-k1000-r1-v9999999-indigo-US-EAST.dat

FILES=../indigo_results/results-weak-k1000-r1-t*-v9999999-uniform/TPSL/*US-EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > tmp_unsorted && sort -n tmp_unsorted  > TPS_T-k1000-r1-v9999999-weak-US-EAST.dat

gnuplot -e "iuse='TPS_T-k1000-r1-v9999999-indigo-US-EAST.dat'" -e "wuse='TPS_T-k1000-r1-v9999999-weak-US-EAST.dat'" \
			plot_generator/TPS_Threads.gnuplot > TPS_T-k1000-r1-v9999999.ps
