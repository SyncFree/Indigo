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

import swift.api.CRDTIdentifier;
import swift.exceptions.IncompatibleTypeException;

public abstract class ResourceDecorator<V extends ResourceDecorator<V, T>, T> implements Resource<T> {

	public Resource<T> originalResource;
	private CRDTIdentifier uid;

	public ResourceDecorator() {
	}

	public ResourceDecorator(CRDTIdentifier uid, Resource<T> resource) {
		this.originalResource = resource;
		this.uid = uid;
	}

	// ATTENTION: To do a merge between the local cache and the value read, it
	// should use some version,
	// because a different algorithm may have multiple versions of the decorated
	// resource.
	// In our algorithm that doesn't happen, but just in case...
	public abstract V createDecoratorCopy(Resource<T> resource) throws IncompatibleTypeException;

	public void initialize(String ownerId, ResourceRequest<T> request) {
		originalResource.initialize(ownerId, request);
	}

	public TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<T> request) {
		return originalResource.transferOwnership(fromId, toId, request);
	}

	public T getSiteResource(String siteId) {
		return originalResource.getSiteResource(siteId);
	}

	@Override
	public T getCurrentResource() {
		return originalResource.getCurrentResource();
	}

	@Override
	public CRDTIdentifier getUID() {
		return uid;
	}

	@Override
	public boolean isOwner(String siteId) {
		return originalResource.isOwner(siteId);
	}

	@Override
	public boolean isReservable() {
		return originalResource.isReservable();
	}

	public Collection<String> getAllResourceOwners() {
		return originalResource.getAllResourceOwners();
	}

	public boolean releaseShare(String ownerId, String masterId) {
		return originalResource.releaseShare(ownerId, masterId);
	}

	public String toString() {
		return originalResource.toString();
	}
}
