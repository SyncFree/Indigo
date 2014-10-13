#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;
set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,20" linewidth 1;
set xlabel "# Threads";
set ylabel "Throughput [ TP/Second ]";
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

set pointinterval 0.1;

#plot    iuse using 1:2  every ::1 title "Indigo: US-E"  with linespoints pi 3 lt 1 ps 2 pt 2 lc rgb '#0000FF',\
 #       iusw using 1:2  every ::1 title "Indigo: US-W"  with linespoints pi 3 lt 1 ps 2 pt 2 lc rgb '#00FF00',\
  #      ieur using 1:2  every ::1 title "Indigo: EUR"   with linespoints pi 3 lt 1 ps 2 pt 2 lc rgb '#FF0000',\
   #     wuse using 1:2  every ::1 title "Causal: US-E"  with linespoints pi 3 lt 2 ps 2 pt 4 lc rgb '#0000FF',\
	#    wusw using 1:2  every ::1 title "Causal: US-W"  with linespoints pi 3 lt 2 ps 2 pt 4 lc rgb '#00FF00',\
     #   weur using 1:2  every ::1 title "Causal: EUR"   with linespoints pi 3 lt 2 ps 2 pt 4 lc rgb '#FF0000';

plot    iallr5w1 using 1:2  every ::1 title "Indigo R5-W1" with linespoints pi 1 lt 1 ps 2 pt 2 lc rgb '#000000',\
        iallr5w2 using 1:2  every ::1 title "Indigo R5-W2" with linespoints pi 1 lt 1 ps 2 pt 4 lc rgb '#0F0F0F',\
        iallr5w3 using 1:2  every ::1 title "Indigo R5-W3" with linespoints pi 1 lt 1 ps 2 pt 6 lc rgb '#030303';
        
        
        
