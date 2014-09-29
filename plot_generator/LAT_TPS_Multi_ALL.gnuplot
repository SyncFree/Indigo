#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;
set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,20" linewidth 1;
set xlabel "Throughput [TP/s]";
set ylabel "Latency [ms]";
set mxtics;
set mytics;
set xr [*:*];
set yr [*:*];

set key right top;
#set grid xtics ytics lt 30 lt 30;
set label;
set clip points;
#set lmargin at screen 0.11;
#set rmargin at screen 0.99;
#set bmargin at screen 0.05;
#set tmargin at screen 0.9999;

#plot    iuse using 1:2  every ::1 title "Indigo: US-E"  with linespoints pi 3 lt 1 ps 2 pt 2 lc rgb '#0000FF',\
 #       iusw using 1:2  every ::1 title "Indigo: US-W"  with linespoints pi 3 lt 1 ps 2 pt 2 lc rgb '#00FF00',\
  #      ieur using 1:2  every ::1 title "Indigo: EUR"   with linespoints pi 3 lt 1 ps 2 pt 2 lc rgb '#FF0000',\
   #     wuse using 1:2  every ::1 title "Causal: US-E"  with linespoints pi 3 lt 2 ps 2 pt 4 lc rgb '#0000FF',\
	#    wusw using 1:2  every ::1 title "Causal: US-W"  with linespoints pi 3 lt 2 ps 2 pt 4 lc rgb '#00FF00',\
     #   weur using 1:2  every ::1 title "Causal: EUR"   with linespoints pi 3 lt 2 ps 2 pt 4 lc rgb '#FF0000';
set pointinterval 1
plot    iall using 2:3   every ::1 title "Indigo: 3 Regions" with linespoints lt 1 ps 2 pt 2 lc rgb '#000000',\
        wall using 2:3   every ::1 title "Causal: 3 Regions" with linespoints lt 2 ps 2 pt 4 lc rgb '#444444',\
        sall using 2:3   every ::1 title "Strong: 1 Regions" with linespoints pi 1 lt 3 ps 2 pt 6 lc rgb '#1F1F1F';


