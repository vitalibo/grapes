package com.github.vitalibo.grapes.processing.infrastructure.vk.impl;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import com.github.vitalibo.grapes.processing.infrastructure.vk.VkClient;
import com.google.gson.*; // NOPMD
import com.vk.api.sdk.client.VkApiClient;
import com.vk.api.sdk.client.actors.UserActor;
import com.vk.api.sdk.httpclient.HttpTransportClient;
import com.vk.api.sdk.objects.friends.responses.GetResponse;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VkClientProxy extends VkClient {

    @Getter
    private final Gson gson;
    @Getter
    private final VkApiClient vk;
    @Getter
    private final UserActor actor;

    public VkClientProxy(UserInputSplit split, Configuration configuration) {
        this(configuration, new Gson(),
            new VkApiClient(
                new HttpTransportClient(
                    configuration.getInt("grapes.vk.proxy.retryAttemptsNetworkErrorCount", 3),
                    configuration.getInt("grapes.vk.proxy.retryAttemptsInvalidStatusCount", 5))),
            new UserActor(
                configuration.getInt(String.format("grapes.vk.proxy.actors.%s.id", split.getId()
                    % (configuration.getPropsWithPrefix("grapes.vk.proxy.actors.").size() / 2)), 0),
                configuration.get(String.format("grapes.vk.proxy.actors.%s.accessToken", split.getId()
                    % (configuration.getPropsWithPrefix("grapes.vk.proxy.actors.").size() / 2)))));
    }

    public VkClientProxy(Configuration configuration, Gson gson, VkApiClient vk, UserActor actor) {
        super(configuration);
        this.gson = gson;
        this.vk = vk;
        this.actor = actor;
    }

    @Override
    @SneakyThrows
    public Map<Integer, List<Integer>> friends(List<Integer> ids) {
        JsonArray response = vk.execute()
            .batch(actor, ids.stream()
                .map(i -> vk.friends()
                    .get(actor)
                    .userId(i))
                .collect(Collectors.toList()))
            .execute()
            .getAsJsonArray();

        return IntStream.range(0, ids.size())
            .mapToObj(i -> new AbstractMap.SimpleEntry<>(ids.get(i), parse(response.get(i))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<Integer> parse(JsonElement element) {
        try {
            return Optional.ofNullable(gson.fromJson(element, GetResponse.class))
                .map(GetResponse::getItems)
                .orElse(Collections.emptyList());

        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

}
