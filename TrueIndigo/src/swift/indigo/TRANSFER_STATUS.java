package swift.indigo;

public enum TRANSFER_STATUS {
	SUCCESS, FAIL, PARTIAL, ALREADY_SATISFIED;

	public boolean hasTransferred() {
		return this.equals(SUCCESS) || this.equals(PARTIAL);
	}
}
