#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;
set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1;
set xlabel "Time [seconds]";
#set ylabel "Latency [ms]";
set mxtics;
set mytics;
set xr [*:*];
set yr [*:*];

set key right bottom;
#set grid xtics ytics lt 30 lt 30;
set label;
set clip points;
#set lmargin at screen 0.11;
#set rmargin at screen 0.99;
#set bmargin at screen 0.05;
#set tmargin at screen 0.9999;

set pointinterval 100;
stats iuse using 1:2
start_e=STATS_min_x
stats iusw using 1:2
start_w=STATS_min_x


plot iuser using ($1-start_e)/1000000000:2 title "Retries US-EAST [#]" with lines lc "#444444", \
iuse using ($1-start_e)/1000000000:2 title "Latency US-EAST [ms]" ps 2 pt 1,\
iuswr using ($1-start_w)/1000000000:2 title "Retries US-WEST [#]" with lines lc "#444444", \
iusw using ($1-start_w)/1000000000:2 title "Latency US-WEST [ms]" ps 2 pt 1\
;


