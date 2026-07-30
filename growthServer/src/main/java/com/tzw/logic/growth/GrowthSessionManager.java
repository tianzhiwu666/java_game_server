package com.tzw.logic.growth;

import com.tzw.mq.MqProducer;
import com.tzw.network.Conn;
import com.tzw.server.GrowthRouter.GrowthEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GrowthSessionManager {

    private static final Logger log = LoggerFactory.getLogger(GrowthSessionManager.class);

    private final ConcurrentHashMap<Long, GrowthActor> actors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ExecutorService> executors = new ConcurrentHashMap<>();

    /** MQ 发布者（依赖注入） */
    private final MqProducer mqProducer;

    public GrowthSessionManager(MqProducer mqProducer) {
        this.mqProducer = mqProducer;
    }

    public GrowthActor getOrCreateActor(long playerId, Conn conn) {
        return actors.computeIfAbsent(playerId, id -> {
            GrowthActor actor = new GrowthActor(id, mqProducer);
            ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "growth-" + id);
                t.setDaemon(true);
                return t;
            });
            executors.put(id, executor);
            executor.submit(() -> {
                try {
                    actor.run();
                } finally {
                    actors.remove(id);
                    executors.remove(id);
                    executor.shutdown();
                    log.info("[GrowthSessionManager] actor {} removed, remaining={}", id, actors.size());
                }
            });
            if (conn != null) {
                actor.onConnect(conn);
            }
            log.info("[GrowthSessionManager] actor {} created", id);
            return actor;
        });
    }

    public GrowthActor getActor(long playerId) {
        return actors.get(playerId);
    }

    public boolean dispatch(long playerId, GrowthEvent event) {
        GrowthActor actor = actors.get(playerId);
        if (actor == null) {
            log.warn("[GrowthSessionManager] actor {} not found, drop event: {}", playerId, event.type());
            return false;
        }
        return actor.tellEvent(event);
    }

    public int getOnlineCount() {
        return actors.size();
    }

    public List<GrowthActor> getAllActors() {
        return new ArrayList<>(actors.values());
    }

    public void stop() {
        log.info("[GrowthSessionManager] stopping all actors, count={}", actors.size());
        for (GrowthActor actor : actors.values()) {
            actor.stop();
        }
        for (ExecutorService executor : executors.values()) {
            executor.shutdownNow();
        }
        actors.clear();
        executors.clear();
    }
}
