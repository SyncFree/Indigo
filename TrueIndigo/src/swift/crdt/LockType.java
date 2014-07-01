package swift.crdt;

public enum LockType {
	EXCLUSIVE_ALLOW, ALLOW, FORBID;

	public boolean isShareable() {
		return !this.equals(EXCLUSIVE_ALLOW);
	}

	public boolean isExclusive() {
		return this.equals(EXCLUSIVE_ALLOW);
	}
}
