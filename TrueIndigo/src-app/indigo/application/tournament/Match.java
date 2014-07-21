package indigo.application.tournament;

public class Match {

    private String matchId;
    private String player1;
    private String player2;

    public Match() {

    }

    public Match(String matchId, String player1, String player2) {
        this.matchId = matchId;
        this.player1 = player1;
        this.player2 = player2;
    }

    public String getMatchId() {
        return matchId;
    }

    public void setMatchId(String matchId) {
        this.matchId = matchId;
    }

    public String getTeam1() {
        return player1;
    }

    public void setPlayer1(String player1) {
        this.player1 = player1;
    }

    public String getPlayer2() {
        return player2;
    }

    public void setPlayer2(String player2) {
        this.player2 = player2;

    }

    @Override
    public int hashCode() {
        return matchId.hashCode() ^ player1.hashCode() ^ player2.hashCode();
    }

}
