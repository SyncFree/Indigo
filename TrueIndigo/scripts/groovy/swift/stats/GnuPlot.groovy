package swift.stats

import swift.deployment.Tools

public class GnuPlot {
    static OPEN_CMD = "open" // System.getenv("SESSION"). contains("ubuntu" )? "xdg-open" : "open"

    static doGraph( output, List script, Map useries, Closure plotstyle ) {
        doGraph( output, script, useries, plotstyle, { a,b ->
            a.compareTo( b )
        })
    }

    static doGraph( output, List script, Map series, Closure plotstyle, Closure sorter) {

        series = series.sort( new Comparator() {
                    public int compare( a, b ) {
                        return sorter.call(a,b)
                    }
                })

        File scriptFile = new File(output + '.gnuplot')

        scriptFile.getParentFile().mkdirs()

        PrintWriter pw = scriptFile.newPrintWriter()

        pw.printf('set output "| ps2pdf - ' + output + '.pdf"\n')

        script.each {
            pw.printf('%s;\n', it)
        }


        def plotline = 'plot '
        series.each { k, v ->
            plotline += String.format(' "-" %s,', plotstyle.call(k,v) )
        }

        pw.println plotline[0..-2]

        series.each { k, List v ->
            v.each { pw.println it }
            pw.println 'end'
        }

        pw.close()


        Tools.exec([
            "/bin/bash",
            "-c",
            "gnuplot " + scriptFile.absolutePath
        ]).waitFor()

        Tools.Sleep(5)


        //        Tools.exec([
        //            "/bin/bash",
        //            "-c",
        //            OPEN_CMD + " " + output + ".pdf"
        //        ]).waitFor()
    }
}
