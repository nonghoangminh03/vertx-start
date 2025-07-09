package org.duydio;

import io.vertx.core.*;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class MainVerticle extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(MainVerticle.class);
    private static final String logFormat = "[%s] Thread: %s - %s";

    private final VertxOptions options;

    public MainVerticle(VertxOptions options) {
        this.options = options;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);

        logger.info(String.format(
                "[start] Thread: %s - eventLoopPoolSize=%d, workerPoolSize=%d",
                Thread.currentThread().getName(),
                options.getEventLoopPoolSize(),
                options.getWorkerPoolSize()
        ));

        System.out.println("Event loop pool size: " + options.getEventLoopPoolSize());
        System.out.println("Worker pool size: " + options.getWorkerPoolSize());

        RedisOptions options = new RedisOptions()
                .setConnectionString("redis://127.0.0.1:6379");

        Redis client = Redis.createClient(vertx, options);

        RedisAPI redis = RedisAPI.api(client);

        router.get("/api/hello").handler(ctx -> {
            ctx.response()
                    .putHeader("content-type", "application/json")
                    .end("{\"message\": \"Hello from Vert.x!\"}");
        });

        router.post("/api/set").handler(ctx -> {
            ctx.request().bodyHandler(buffer -> {
                // parse JSON
                UUID uuid = UUID.randomUUID();
                long r = random()* 1000L;
                logger.info(String.format(
                        logFormat,
                        LocalDateTime.now(),
                        currentThread(),
                        "start " + uuid + " " + r
                ));

                JsonObject body = buffer.toJsonObject();
                String key = body.getString("key");
                String value = body.getString("value");

                vertx.setTimer(r, id -> {
                    set(redis, key, value, uuid).onSuccess(result -> {
                        ctx.response()
                                .putHeader("content-type", "application/json")
                                .end(new JsonObject().put("status", "ok").toBuffer());
                    }).onFailure(cause -> {
                        ctx.response()
                                .setStatusCode(500)
                                .end(new JsonObject().put("error", cause.getMessage()).toBuffer());
                    });
                });
            });
        });

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(8888, http -> {
                    if (http.succeeded()) {
                        startPromise.complete();
                        System.out.println("HTTP server started on port 8888");
                    } else {
                        startPromise.fail(http.cause());
                    }
                });
    }

    private Future<Boolean> set(RedisAPI redis, String key, String value, UUID id) {
        Promise<Boolean> promise = Promise.promise();
        redis.set(List.of(key, value), res -> {
            if (res.succeeded()) {
                logger.info(String.format(logFormat, LocalDateTime.now(), currentThread(), "SET OK " + id));
                promise.complete(true);
            } else {
                logger.error(String.format(logFormat, LocalDateTime.now(), currentThread(), "SET ERROR " + id), res.cause());
                promise.complete(false);
            }
        });
        return promise.future();
    }

    private String currentThread() {
        return Thread.currentThread().getName();
    }

    private int random() {
        Random random = new Random();
        return random.nextInt(5) + 1;
    }


    public static void main(String[] args) {

        VertxOptions options = new VertxOptions()
                .setEventLoopPoolSize(4)
                .setWorkerPoolSize(50);

        Vertx.vertx(options).deployVerticle(new MainVerticle(options));
    }

}
