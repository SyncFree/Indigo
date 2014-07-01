package lib

import java.util.concurrent.atomic.AtomicInteger
import static lib.Tools.*
import static lib.Topology.*

class SwiftSocial extends SwiftBase {
    static String INITDB_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties swift.application.social.SwiftSocialBenchmark"
    static String SCOUT_CMD = "-cp swiftcloud.jar -Djava.util.logging.config.file=logging.properties swift.application.social.SwiftSocialBenchmark"

    static int initDB( String client, String server, String config, String heap = "512m") {
        println "CLIENT: " + client + " SERVER: " + server + " CONFIG: " + config

        def cmd = "-Dswiftsocial=" + config + " " + INITDB_CMD + " init -server " + server + " "
        def res = rshC( client, swift_app_cmd_nostdout("-Xmx" + heap, cmd, "initdb-stderr.txt", "initdb-stdout.txt")).waitFor()
        println "OK.\n"
        return res
    }
   
    static void runScouts( List scoutGroups, String config, String shepard, int threads, String heap ="512m" ) {
        def hosts = []

        scoutGroups.each{ hosts += it.all() }

        println hosts

        AtomicInteger n = new AtomicInteger();
        def resHandler = { host, res ->
            def str = n.incrementAndGet() + "/" + hosts.size() + (res < 1 ? " [ OK ]" : " [FAILED]") + " : " + host
            println str
        }

        scoutGroups.each { grp ->
            Thread.startDaemon {
                def cmd = { host ->
                    int index = hosts.indexOf( host );
                    String partition = index + "/" + hosts.size()
                    def res = "nohup java -Xmx" + heap + " -Dswiftsocial=" + config + " " + SCOUT_CMD + " run -shepard " + shepard + " -threads " + threads + " -partition " + partition + " -server "
                    res += " " + grp.dc.surrogates[index % grp.dc.surrogates.size()]
                    res += " > scout-stdout.txt 2> scout-stderr.txt < /dev/null & sleep 1; tail -f scout-stderr.txt &"
                    return res;
                }
                grp.deploy( cmd, resHandler)
            }
        }
    }



    static final DEFAULT_PROPS = [
        'swift.cacheEvictionTimeMillis':'5000000',
        'swift.maxCommitBatchSize':'10',
        'swift.maxAsyncTransactionsQueued':'50',
        'swift.cacheSize':'256',
        'swift.asyncCommit':'true',
        'swift.notifications':'true',
        'swift.cachePolicy':'CACHED',
        'swift.isolationLevel':'SNAPSHOT_ISOLATION',
        'swift.computeMetadataStatistics':'true',
        'swiftsocial.numUsers':'1000',
        'swiftsocial.userFriends':'25',
        'swiftsocial.biasedOps':'9',
        'swiftsocial.randomOps':'1',
        'swiftsocial.opGroups':'10000',
        'swiftsocial.recordPageViews':'false',
        'swiftsocial.thinkTime':'0'
    ]
}
