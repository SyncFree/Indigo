package sys.shepard.proto;

public class GrazingGranted {

	int duration;
	int when;

	public GrazingGranted() {
	}

	public GrazingGranted(int duration, int when) {
		this.duration = duration;
		this.when = when;
	}

	public int duration() {
		return duration;
	}

	public int when() {
		return when;
	}
}
