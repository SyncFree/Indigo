sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t1-v5000-uniform/micro_benchmark_results_US-EAST.log | awk '{print $2" "$4" "$5}' > tmp0e & \
sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t1-v5000-uniform/remote_indigo_results_US-EAST.log | awk '{print $2" "$6}' > tmp1e & \
sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t1-v5000-uniform/micro_benchmark_results_US-WEST.log | awk '{print $2" "$4" "$5}' > tmp0w & \
sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t1-v5000-uniform/remote_indigo_results_US-WEST.log | awk '{print $2" "$6}' > tmp1w & \
	gnuplot  -e "iuse='tmp0e' ; iuser='tmp1e' ; iusw='tmp0w' ; iuswr='tmp1w'" plot_generator/plot_over_time.gnuplot > plot_over_time-k1-r2-t1-v5000.ps

sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t5-v10000-uniform/micro_benchmark_results_US-EAST.log | awk '{print $2" "$4" "$5}' > tmp2 & \
sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t5-v10000-uniform/remote_indigo_results_US-EAST.log | awk '{print $2" "$6}' > tmp3 & \
	gnuplot  -e "iuse='tmp2' ; iuser='tmp3'" plot_generator/plot_over_time.gnuplot > plot_over_time-k1-r2-t5-v10000.ps

sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t10-v20000-uniform/micro_benchmark_results_US-EAST.log | awk '{print $2" "$4" "$5}' > tmp4 & \
sed '1,100d'  ../indigo_results/results-indigo-k1-r2-t10-v20000-uniform/remote_indigo_results_US-EAST.log | awk '{print $2" "$6}' > tmp5 & \
	gnuplot  -e "iuse='tmp4' ; iuser='tmp5'" plot_generator/plot_over_time.gnuplot > plot_over_time-k1-r2-t10-v20000.ps

rm tmp0 tmp1 tmp2 tmp3 tmp4 tmp5