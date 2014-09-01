gnuplot -e "indigo='/Users/balegas/workspace/java/indigo_results/results-indigo-k100-r1-t10-v1000-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "weak='/Users/balegas/workspace/java/indigo_results/results-weak-k100-r1-t10-v1000-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > k100-r1-t10-v1000_CDF.ps

gnuplot -e "indigo='/Users/balegas/workspace/java/indigo_results/results-indigo-k100-r1-t1-v1000-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "weak='/Users/balegas/workspace/java/indigo_results/results-weak-k100-r1-t1-v1000-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > k100-r1-t1-v1000_CDF.ps

gnuplot -e "indigo='/Users/balegas/workspace/java/indigo_results/results-indigo-k1-r1-t1-v1000-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
	 	-e "weak='/Users/balegas/workspace/java/indigo_results/results-weak-k1-r1-t1-v1000-uniform/CDF/micro_benchmark_results_US-EAST.dat'" \
			plot_generator/latencyCDF-Per-Semantics.gnuplot > k1-r1-t1-v1000_CDF.ps

FILES=../indigo_results/results-indigo-k100-r1-t*-v1000-uniform/TPSL/*US-EAST*
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > TPS_T-k100-r1-v1000-indigo.dat

FILES=../indigo_results/results-weak-k100-r1-t*-v1000-uniform/TPSL/*US-EAST* 
java -classpath ./bin/:./TrueIndigo/lib/* evaluation.StatisticsUtils -tps -t $FILES > TPS_T-k100-r1-v1000-weak.dat

sleep 1

gnuplot -e "indigo='TPS_T-k100-r1-v1000-indigo.dat'" -e "weak='TPS_T-k100-r1-v1000-weak.dat'" \
			plot_generator/TPS_Threads.gnuplot > TPS_T-k100-r1-t1-v1000_CDF.ps


