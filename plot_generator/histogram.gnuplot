set term postscript noenhanced eps font "Helvetica,28"
set output "histoerrorbartest.eps"
set boxwidth 0.9
set style fill solid 1.00 border 0
set style histogram errorbars gap 2 lw 1
set style data histograms
set xtics rotate by -20
set bars 0.5
set yrange [*:*] 
set ytics 0,300,2000;
set ylabel "Latency [ms]" font ",40"
set xtic font ",22"
set key left top

plot 'tournament.dat' using 2:4:5:xtic(1) every ::1 ti "US-EAST" lc rgb "red",\
     ''               using 6:8:9:xtic(1)  every ::1 ti "US-WEST" lc rgb "green",\
     ''               using 10:12:13:xtic(1) every ::1 ti "EUROPE" lc rgb "blue",\
     ''               using 14:16:17:xtic(1) every ::1 ti "GLOBAL" lc rgb "yellow";



    
