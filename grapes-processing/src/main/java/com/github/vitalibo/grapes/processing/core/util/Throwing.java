package com.github.vitalibo.grapes.processing.core.util;

import lombok.SneakyThrows;

public interface Throwing {

    static <T, R> java.util.function.Function<T, R> function(Function<T, R> function) {
        return function;
    }

    @FunctionalInterface
    interface Function<T, R> extends java.util.function.Function<T, R> {

        R applyThrow(T t) throws Exception;

        @Override
        @SneakyThrows
        default R apply(T t) {
            return applyThrow(t);
        }

    }

}
