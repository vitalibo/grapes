package com.github.vitalibo.grapes.processing.infrastructure.conf;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public final class HoconConfiguration {

    private HoconConfiguration() {
    }

    public static Configuration parseHocon(Config config, String name) {
        Config resolved = config.getConfig(name)
            .withFallback(config.getConfig("default"))
            .resolve();

        return flattenMap("", resolved.root().unwrapped())
            .collect(Configuration::new, (m, v) -> m.set(v.getKey(), String.valueOf(v.getValue())), Configuration::addResource);
    }

    private static Stream<Map.Entry<String, Object>> flattenMap(String root, Map<String, Object> config) {
        return config.entrySet().stream()
            .flatMap(entry -> flattenMap(root + entry.getKey(), entry.getValue()));
    }

    @SuppressWarnings("unchecked")
    private static Stream<Map.Entry<String, Object>> flattenMap(String key, Object value) {
        if (value instanceof Map) {
            return flattenMap(key + ".", (Map<String, Object>) value);
        } else if (value instanceof Collection) {
            return entry(key, StringUtils.join(',', (Iterable<?>) value));
        }

        return entry(key, value);
    }

    private static <K, V> Stream<Map.Entry<K, V>> entry(K key, V value) {
        return Stream.of(new AbstractMap.SimpleEntry<>(key, value));
    }

}
