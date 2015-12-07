/**
-------------------------------------------------------------------

Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

-------------------------------------------------------------------
 **/
package swift.indigo;

import java.util.Collection;
import java.util.List;
import java.util.Queue;

import swift.api.CRDTIdentifier;
import swift.utils.Pair;

public interface Resource<T> {

	void initialize(String ownerId, ResourceRequest<T> request);

	TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<T> request);

	void apply(String siteId, ResourceRequest<T> req);

	CRDTIdentifier getUID();

	T getCurrentResource();

	T getSiteResource(String siteId);

	boolean isReservable();

	boolean checkRequest(String ownerId, ResourceRequest<T> request);

	boolean isOwner(String siteId);

	public Queue<Pair<String, T>> preferenceList();

	public Queue<Pair<String, T>> preferenceList(String excludeSiteId);

	Collection<String> getAllResourceOwners();

	boolean isSingleOwner(String siteId);

	boolean releaseShare(String ownerId, String masterId);

	default boolean overThreshold(String ownerId, ResourceRequest<T> request) {
		return false;
	}

	ResourceRequest<T> transferOwnershipPolicy(String siteId, ResourceRequest<T> request);

	List<Pair<String, ResourceRequest<T>>> provisionPolicy(String siteId, ResourceRequest<T> request);

	boolean remoteRequiresReservations(ResourceRequest<T> request);

}
