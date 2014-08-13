package swift.indigo;

public enum TRANSFER_STATUS {
	SUCCESS, FAIL, PARTIAL;

	public boolean hasTransferred() {
		return this.equals(SUCCESS) || this.equals(PARTIAL);
	}
}
