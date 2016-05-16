package org.apache.ambari.view.hive2.internal;

import com.google.common.base.Optional;

/**
 * Simple implementation of a container class which can
 * hold one of two values
 *
 * Callers should check if the value if left or right before
 * trying to get the value
 *
 * @param <L> Left Value
 * @param <R> Right value
 */
public class Either<L,R> {

    private final Optional<L> left;
    private final Optional<R> right;


    public boolean isLeft(){
       return left.isPresent() && !right.isPresent();
    }

    public boolean isRight(){
        return !left.isPresent() && right.isPresent();
    }

    public L getLeft(){
        return left.orNull();
    }

    public R getRight(){
        return right.orNull();
    }


    private Either(Optional<L> left, Optional<R> right) {
        this.left = left;
        this.right = right;
    }



    public static <L,R> Either<L,R> left(L value) {
        return new Either<>(Optional.of(value), Optional.<R>absent());
    }

    public static <L,R> Either<L,R> right(R value) {
        return new Either<>(Optional.<L>absent(), Optional.of(value));
    }

    public static <L,R> Either<L,R> none() {
        return new Either<>(Optional.<L>absent(), Optional.<R>absent());
    }

}



