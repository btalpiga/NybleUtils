package com.nyble.util;

import java.io.Serializable;

public class Pair<U, T> implements Serializable {
    public U leftSide;
    public T rightSide;

    public Pair(U leftSide, T rightSide) {
        this.leftSide = leftSide;
        this.rightSide = rightSide;
    }
}
