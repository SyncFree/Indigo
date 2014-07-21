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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.thoughtworks.xstream.core.util.Base64Encoder;

abstract public class Workload implements Iterable<String>, Iterator<String> {

    static List<String> adCopies = new ArrayList<String>();
    static List<String> ads = new ArrayList<String>();
    static List<String> adsData = new ArrayList<String>();
    static int nCopies;

    protected Workload() {
    }

    abstract public int size();

    /*
     * Generates Ad IDs and assign them the maximum view count relative to
     * ViewCountTotal. Creates copies of the same AD with other maximum view
     * count relative to maxViewCountCopy
     */
    public static List<String> populate(int numAds, int numCopies, int maxViewCountAd, int maxViewCountAdCopy) {
        nCopies = numCopies;
        Random rg = new Random(6L);
        for (int i = 0; i < numAds; i++) {
            byte[] tmp = new byte[6];
            rg.nextBytes(tmp);
            Base64Encoder enc = new Base64Encoder();
            String adTitle = enc.encode(tmp);
            ads.add(adTitle);
            // String groupLine = String.format("Ad id;%s; total view count;%d",
            // adTitle,
            // maxViewCountAd / 2 + rg.nextInt(maxViewCountAd / 2));
            String groupLine = String.format("Ad id;%s; total view count;%d", adTitle, maxViewCountAd);
            adsData.add(groupLine);
            for (int j = 1; j <= numCopies; j++) {
                String adCopy = j + "_" + adTitle;
                // int viewCount = maxViewCountAdCopy / 2 +
                // rg.nextInt(maxViewCountAdCopy / 2);
                int viewCount = maxViewCountAdCopy;
                String adLine = String.format("Ad copy id;%s;view count;%d", adCopy, viewCount);
                adCopies.add(adCopy);
                adsData.add(adLine);
            }
        }
        return adsData;
    }

    /*
     * Represents an abstract command the user performs. Each command has a
     * frequency/probability and need to be formatted into a command line.
     */
    static abstract class Operation {
        int frequency;

        Operation freq(int f) {
            this.frequency = f;
            return this;
        }

        abstract String doLine(Random rg, int site);

        public String toString() {
            return getClass().getSimpleName();
        }
    }

    /*
     * View an ad. Ad is chosen in runtime.
     */
    static class ViewAd extends Operation {

        public String doLine(Random rg, int site) {
            return String.format("view_ad;%d;%d", site, nCopies);
        }
    }

    static Operation[] ops = new Operation[] { new ViewAd().freq(100) };

    static AtomicInteger doMixedCounter = new AtomicInteger(7);

    static public Workload doMixed(int site, final int numOps, final int number_of_sites) {
        final Random rg = new Random(doMixedCounter.addAndGet(13 + site));
        final int finalSite = site < 0 ? rg.nextInt(number_of_sites) : site; // fix
                                                                             // site

        return new Workload() {
            Iterator<String> it = null;

            void refill() {
                ArrayList<String> group = new ArrayList<String>();
                for (int i = 0; i < numOps; i++) {
                    group.add(new ViewAd().doLine(rg, finalSite));
                }

                it = group.iterator();
            }

            @Override
            public boolean hasNext() {
                if (it == null)
                    refill();

                return it.hasNext();
            }

            @Override
            public String next() {
                return it.next();
            }

            @Override
            public void remove() {
                throw new RuntimeException("On demand worload generation; remove is not supported...");
            }

            public int size() {
                return numOps;
            }

        };
    }

    public Iterator<String> iterator() {
        return this;
    }

    public static void main(String[] args) throws Exception {

        List<String> resList = Workload.populate(10, 3, 500, 500);

        for (String i : resList)
            System.out.println(i);

        Workload res = Workload.doMixed(3, 10, 3);
        System.out.println(res.size());
        for (String i : res)
            System.out.println(i);

    }
}
