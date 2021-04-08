package com.github.vitalibo.grapes.processing.infrastructure.vk;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import com.github.vitalibo.grapes.processing.infrastructure.vk.impl.VkClientProxy;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class VkClient {

    private final int batchSize;

    public VkClient(Configuration configuration) {
        batchSize = configuration.getInt("grapes.vk.batchSize", 25);
    }

    public Map<Integer, List<Integer>> friends(Supplier<Integer> supplier) {
        return friends(
            Stream.generate(supplier)
                .limit(batchSize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    public abstract Map<Integer, List<Integer>> friends(List<Integer> ids);

    @SneakyThrows
    public static VkClient newInstance(UserInputSplit split, Configuration configuration) {
        Class<? extends VkClient> cls = configuration.getClass("grapes.vk.clientClass", VkClientProxy.class, VkClient.class);
        return cls.getConstructor(UserInputSplit.class, Configuration.class).newInstance(split, configuration);
    }

}
