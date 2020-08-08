package org.apache.lucene.analysis.combo.util;

public class Tuple<S, T> {

    private S v1;
    private T v2;

    public static <A, B> Tuple<A, B> of(A v1, B v2) {
        return new Tuple<A, B>(v1, v2);
    }

    public Tuple(S v1, T v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public Tuple() {
    }

    public S v1() {
        return v1;
    }

    public T v2() {
        return v2;
    }

    public S getV1() {
        return v1;
    }

    public T getV2() {
        return v2;
    }

    public void setV1(S v1) {
        this.v1 = v1;
    }

    public void setV2(T v2) {
        this.v2 = v2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple tuple = (Tuple) o;

        if (v1 != null ? !v1.equals(tuple.v1) : tuple.v1 != null) {
            return false;
        }
        if (v2 != null ? !v2.equals(tuple.v2) : tuple.v2 != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + (v2 != null ? v2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "v1=" + v1 +
                ", v2=" + v2 +
                '}';
    }
}
