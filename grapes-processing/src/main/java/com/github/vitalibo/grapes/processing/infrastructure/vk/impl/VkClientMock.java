package com.github.vitalibo.grapes.processing.infrastructure.vk.impl;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import com.github.vitalibo.grapes.processing.infrastructure.vk.VkClient;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VkClientMock extends VkClient {

    private final Random random;

    public VkClientMock(UserInputSplit split, Configuration configuration) {
        this(configuration, Optional
            .ofNullable(configuration.get(
                String.format("grapes.vk.mock.pseudorandomSeed.%s", split.getId())))
            .map(i -> new Random(Long.parseLong(i)))
            .orElse(new Random()));
    }

    public VkClientMock(Configuration configuration, Random random) {
        super(configuration);
        this.random = random;
    }

    @Override
    public Map<Integer, List<Integer>> friends(List<Integer> ids) {
        return ids.stream()
            .collect(Collectors.toMap(id -> id, ignore -> friends()));
    }

    private List<Integer> friends() {
        return IntStream.range(0, random.nextInt(150))
            .mapToObj(i -> random.nextInt(9_999) + 1)
            .collect(Collectors.toList());
    }

}
