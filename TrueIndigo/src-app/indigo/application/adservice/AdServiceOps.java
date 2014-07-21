/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package indigo.application.adservice;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.crdt.AddWinsSetCRDT;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.LowerBoundCounterCRDT;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.ResourceRequest;
import sys.utils.Tasks;

// implements the ad service functionality

public class AdServiceOps<E> {

    private static Logger logger = Logger.getLogger("indigo.adservice");

    private String siteId;
    private Indigo stub;

    // Warning: workload is not deterministic!!
    private Random rg;

    public AdServiceOps(Indigo stub, String siteId) {
        this.stub = stub;
        this.siteId = siteId;
        this.rg = new Random();
    }

    public Indigo stub() {
        return stub;
    }

    // Populates the databases with Ads, and all permissions assigned to the
    // local replica
    @SuppressWarnings("unchecked")
    void addAd(final String adTitle) {
        try {
            stub.get(NamingScheme.forAd(adTitle), true, BoundedCounterAsResource.class);
            AddWinsSetCRDT<String> index = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forAdIndex(), true,
                    AddWinsSetCRDT.class);
            index.add(adTitle);
            logger.info("Created AD: " + adTitle);

        } catch (Exception e) {
            logger.warning(e.getMessage());
        }
    }

    // Populates the databases with Ads, and all permissions assigned to the
    // local replica
    @SuppressWarnings("unchecked")
    void addAdCopy(final String adTitle, final int site) {
        try {
            stub.get(NamingScheme.forAdCopy(adTitle), true, BoundedCounterAsResource.class);
            AddWinsSetCRDT<String> index = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forAdSiteIndex(site), true,
                    AddWinsSetCRDT.class);
            index.add(adTitle);
            logger.info("Created AD Copy: " + adTitle);

        } catch (Exception e) {
            logger.warning(e.getMessage());
        }
    }

    void setAdInitialValue(final String adTitle, final int maximumViews) {
        try {
            BoundedCounterAsResource counter = stub.get(NamingScheme.forAd(adTitle), true,
                    BoundedCounterAsResource.class);
            counter.increment(maximumViews, siteId);
        } catch (Exception e) {
            logger.warning(e.getMessage());
        }
    }

    void setAdCopyInitialValue(final String adTitle, final int maximumViews) {
        try {
            BoundedCounterAsResource counter = stub.get(NamingScheme.forAdCopy(adTitle), true,
                    BoundedCounterAsResource.class);
            counter.increment(maximumViews, siteId);
        } catch (Exception e) {
            logger.warning(e.getMessage());
        }
    }

    public AdServiceOpsResults viewAd(final int site, final int numCopies, final boolean onlyGlobal) {
        try {
            final AdServiceOpsResults result;
            // First transaction selects the AD to view, from the copies that
            // are available at "site"
            final String adId = selectAd(site);
            // View the selected Ad, has no guarantees that the ad can be viewed
            if (adId != null) {
                result = viewAd(adId, onlyGlobal);
            } else {
                logger.severe("No more ads available");
                return new AdServiceOpsResults("NO_ID", ViewResponseCode.NO_ADS, -1, -1);
            }

            // Asynchronously updates the indexes
            switch (result.code) {

            case OK:
                return result;
            case AD_ZERO:
                Tasks.exec(0, new Runnable() {
                    @Override
                    public void run() {
                        stub.beginTxn();
                        try {
                            updateAdIndex(adId, numCopies);
                        } catch (SwiftException e) {
                            e.printStackTrace();
                        }
                        stub.endTxn();
                    }
                });
                break;
            case COPY_ZERO:
                Tasks.exec(0, new Runnable() {
                    @Override
                    public void run() {
                        stub.beginTxn();
                        try {
                            updateCopyIndex(adId, site);
                        } catch (SwiftException e) {
                            e.printStackTrace();
                        }
                        stub.endTxn();
                    }
                });
                break;
            default:
                logger.severe("Something bad occurred!");
                break;
            }
            return result;
        } catch (SwiftException e) {
            e.printStackTrace();
        }
        return new AdServiceOpsResults("NO_ID", ViewResponseCode.OTHER, -1, -1);
    }

    public AdServiceOpsResults viewAd(final String copyId, final boolean onlyGlobal) {
        try {
            ViewResponseCode result = null;
            String adId = copyId.split("_")[1];

            List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
            if (!onlyGlobal)
                resources.add(new CounterReservation(siteId, NamingScheme.forAdCopy(copyId), 1));
            resources.add(new CounterReservation(siteId, NamingScheme.forAd(adId), 1));

            stub.beginTxn(resources);

            // Update the global view count
            BoundedCounterAsResource globalViewCount = stub.get(NamingScheme.forAd(adId), true,
                    BoundedCounterAsResource.class);

            System.err.println("----------------------------------->>>>>>" + globalViewCount);
            if (globalViewCount.getValue() > 0) {
                boolean success = globalViewCount.decrement(1, siteId);
                System.out.println("VIEW COUNT " + globalViewCount.getValue());
                if (!success)
                    logger.log(Level.SEVERE, "Decrement failed after granting permission" + adId);
            } else if (globalViewCount.getValue() <= 0) {
                result = ViewResponseCode.AD_ZERO;
            }
            int copyValue = -1;
            if (!onlyGlobal) {
                // Update the copy view count
                LowerBoundCounterCRDT copyViewCount = stub.get(NamingScheme.forAdCopy(copyId), true,
                        LowerBoundCounterCRDT.class);
                if (copyViewCount.getValue() > 0) {
                    boolean success = copyViewCount.decrement(1, siteId);
                    if (!success)
                        logger.log(Level.SEVERE, "Decrement failed after granting permission" + adId);
                } else if (copyViewCount.getValue() <= 0) {
                    result = (result == null) ? ViewResponseCode.COPY_ZERO : result;
                }
                copyValue = copyViewCount.getValue();
            }
            stub.endTxn();
            logger.info("VIEW_AD " + globalViewCount.getValue() + " " + copyValue);
            result = (result == null) ? ViewResponseCode.OK : result;
            return new AdServiceOpsResults(copyId, result, copyValue, globalViewCount.getValue());
        } catch (Exception e) {
            e.printStackTrace();
            stub.abortTxn();
            logger.warning(e.getMessage());
            return new AdServiceOpsResults(copyId, ViewResponseCode.ABORTED, -1, -1);
        }
    }

    @SuppressWarnings("unchecked")
    public String selectAd(int siteId) throws SwiftException {
        stub.beginTxn();
        AddWinsSetCRDT<String> adIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forAdSiteIndex(siteId), true,
                AddWinsSetCRDT.class);
        stub.endTxn();
        if (adIndex.size() > 0) {
            String[] array = adIndex.getValue().toArray(new String[0]);
            return array[rg.nextInt(array.length)];
        } else {
            return null;
        }

    }

    @SuppressWarnings("unchecked")
    public void updateAdIndex(String ad, int numCopies) throws SwiftException {
        AddWinsSetCRDT<String> adIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forAdIndex(), true,
                AddWinsSetCRDT.class);
        for (int i = 0; i < numCopies; i++) {
            updateCopyIndex(ad, i);
        }
        adIndex.remove(ad);
    }

    @SuppressWarnings("unchecked")
    public void updateCopyIndex(String ad, int site) throws SwiftException {
        AddWinsSetCRDT<String> adIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forAdSiteIndex(site), true,
                AddWinsSetCRDT.class);
        adIndex.remove(ad);
    }

    static enum ViewResponseCode {
        COPY_ZERO, ABORTED, OK, AD_ZERO, OTHER, NO_ADS
    }

    static class AdServiceOpsResults implements Results {

        ViewResponseCode code;
        String adId;
        int copyValue;
        int globalValue;
        long txnStartTime, txnEndTime;
        int sessionId;

        protected AdServiceOpsResults(String adId, ViewResponseCode type, int copyValue, int globalValue) {
            this.adId = adId;
            this.code = type;
            this.copyValue = copyValue;
            this.globalValue = globalValue;
        }

        public Results setStartTime(long start) {
            this.txnStartTime = start;
            this.txnEndTime = System.currentTimeMillis();
            return this;
        }

        @Override
        public Results setSession(int sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public String getLogRecord() {
            final long txnExecTime = txnEndTime - txnStartTime;
            return String.format("%d,%s,%s,%d,%d,%d,%d", sessionId, adId, code, copyValue, globalValue, txnExecTime,
                    txnEndTime);
        }

        @Override
        public void logTo(PrintStream out) {
            out.println(getLogRecord());
        }

    }

}
