package com.nyble.util;

import java.io.Serializable;
import java.util.Objects;

public class Pair<U, T> implements Serializable {
    public U leftSide;
    public T rightSide;

    public Pair(U leftSide, T rightSide) {
        this.leftSide = leftSide;
        this.rightSide = rightSide;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pair)) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(leftSide, pair.leftSide) &&
                Objects.equals(rightSide, pair.rightSide);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftSide, rightSide);
    }
}
