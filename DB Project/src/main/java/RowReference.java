import java.io.Serializable;

public class RowReference implements Serializable, Comparable {
    private int pageNO;
    private Comparable clusteringKey;

    public RowReference(int pageNO, Comparable clusteringKey){
        this.pageNO = pageNO;
        this.clusteringKey = clusteringKey;
    }

    public int getPageNO() {
        return pageNO;
    }


    public Comparable getClusteringKey() {
        return clusteringKey;
    }

    @Override
    public int compareTo(Object o) {
        RowReference r = (RowReference) o;
        return getPageNO() == r.getPageNO() ? (getClusteringKey().compareTo(r.getClusteringKey()) > 0? 1 : -1) : getPageNO() - r.getPageNO();
    }
}
