package net.bndy.ftsi;

public class IndexStatus {
    private int num;
    private int numDeleted;
    private int total;

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int getNumDeleted() {
        return numDeleted;
    }

    public void setNumDeleted(int numDeleted) {
        this.numDeleted = numDeleted;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public IndexStatus(int num, int numDeleted, int total) {
        this.num = num;
        this.numDeleted = numDeleted;
        this.total = total;
    }
}
