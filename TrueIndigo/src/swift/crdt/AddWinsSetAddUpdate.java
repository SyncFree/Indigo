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
package swift.crdt;

import java.util.Set;

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTUpdate;

public class AddWinsSetAddUpdate<V, T extends AbstractAddWinsSetCRDT<V, T>> implements CRDTUpdate<T> {
    protected V val;
    protected TripleTimestamp instance;
    // WISHME: represent it more efficiently using vectors if possible.
    // That would require some substantial API chances, since it's not as easy
    // as using dependenceClock (for example, it can be overapproximated), there
    // are holes in here as well.
    protected Set<TripleTimestamp> overwrittenAdds;

    // required for kryo
    public AddWinsSetAddUpdate() {
    }

    public AddWinsSetAddUpdate(V val, TripleTimestamp instance, Set<TripleTimestamp> overwrittenAdds) {
        this.instance = instance;
        this.val = val;
        this.overwrittenAdds = overwrittenAdds;
    }

    public V getVal() {
        return this.val;
    }

    @Override
    public void applyTo(T crdt) {
        crdt.applyAdd(val, instance, overwrittenAdds);
    }

    @Override
    public Object getValueWithoutMetadata() {
        return val;
    }
}
