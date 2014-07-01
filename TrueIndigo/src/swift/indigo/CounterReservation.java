package swift.indigo;

import swift.api.CRDTIdentifier;

public class CounterReservation {

	private CRDTIdentifier id;
	private int amount;

	public CounterReservation() {

	}

	public CounterReservation(CRDTIdentifier id, int amount) {
		this.id = id;
		this.amount = amount;
	}

	public CRDTIdentifier getId() {
		return id;
	}

	public void setId(CRDTIdentifier id) {
		this.id = id;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String toString() {
		return "{" + id + ", " + amount + "}";
	}
}
