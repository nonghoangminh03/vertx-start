package org.duydio;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;

import java.time.LocalDateTime;
import java.util.List;

public class RedisVerticle extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(RedisVerticle.class);
    private RedisAPI redis;


    @Override
    public void start(Promise<Void> startPromise) {
        RedisOptions options = new RedisOptions()
                .setConnectionString("redis://127.0.0.1:6379");

        Redis client = Redis.createClient(vertx, options);
        redis = RedisAPI.api(client);


        // ĐĂNG KÝ CONSUMER CHO redis.set
        vertx.eventBus().consumer("redis.set", msg -> {
            JsonObject body = (JsonObject) msg.body();
            String key = body.getString("key");
            String value = body.getString("value");

            logger.info(String.format("[%s] RedisVerticle running on thread: %s",
                    LocalDateTime.now(), Thread.currentThread().getName()));

            redis.set(List.of(key, value), res -> {
                if (res.succeeded()) {
                    msg.reply(new JsonObject().put("status", "ok"));
                } else {
                    msg.fail(500, res.cause().getMessage());
                }
            });
        });

        startPromise.complete();
    }
}
