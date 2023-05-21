import java.io.Serializable;
import java.util.Objects;

public class Point implements Serializable {
    private Comparable x, y, z;

    public Point(Comparable x, Comparable y, Comparable z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Comparable getX() {
        return x;
    }

    public Comparable getY() {
        return y;
    }

    public Comparable getZ() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Objects.equals(x, point.x) && Objects.equals(y, point.y) && Objects.equals(z, point.z);
    }
}
