package swift.crdt;

public enum ShareableLock {
    EXCLUSIVE_ALLOW, ALLOW, FORBID, NONE;

    public boolean isShareable() {
        return !this.equals(EXCLUSIVE_ALLOW);
    }

    public boolean isExclusive() {
        return this.equals(EXCLUSIVE_ALLOW);
    }

    public boolean isCompatible(ShareableLock resource) {
        return this.equals(resource) && !this.equals(EXCLUSIVE_ALLOW);
    }

}
