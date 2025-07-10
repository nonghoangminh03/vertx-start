package org.duydio;

import io.vertx.core.*;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;

import java.time.LocalDateTime;
import java.util.ArrayList;
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
        logger.info(String.format(
                "[start] Thread: %s",
                Thread.currentThread().getName()
        ));

        Router router = Router.router(vertx);

        router.get("/api/hello").handler(ctx -> {
            ctx.response()
                    .putHeader("content-type", "application/json")
                    .end("{\"message\": \"Hello from Vert.x!\"}");
        });

        router.post("/api/set").handler(ctx -> {
            ctx.request().bodyHandler(buffer -> {
                UUID uuid = UUID.randomUUID();
                long r = random() * 1000L;

                logger.info(String.format(
                        logFormat,
                        LocalDateTime.now(),
                        currentThread(),
                        "start " + uuid + " delay=" + r
                ));

                JsonObject body = buffer.toJsonObject();

                vertx.setTimer(r, id -> {
                    vertx.eventBus().request("redis.set", body, reply -> {
                        if (reply.succeeded()) {
                            logger.info(String.format(
                                    logFormat,
                                    LocalDateTime.now(),
                                    currentThread(),
                                    "SET OK " + uuid
                            ));
                            ctx.response()
                                    .putHeader("content-type", "application/json")
                                    .end(reply.result().body().toString());
                        } else {
                            logger.error(String.format(
                                    logFormat,
                                    LocalDateTime.now(),
                                    currentThread(),
                                    "SET ERROR " + uuid
                            ), reply.cause());
                            ctx.response()
                                    .setStatusCode(503)
                                    .end(new JsonObject()
                                            .put("error", reply.cause().getMessage())
                                            .toBuffer());
                        }
                    });
                });
            });
        });

        router.post("/api/many").handler(ctx -> {
            List<Future> futures = new ArrayList<>();

            for (int i = 0; i < 20; i++) {
                UUID uuid = UUID.randomUUID();
                JsonObject payload = new JsonObject()
                        .put("key", "key-" + uuid)
                        .put("value", "value-" + uuid);

                Promise<Void> promise = Promise.promise();
                futures.add(promise.future());

                long r = random() * 1000L;
                vertx.setTimer(r, id -> {
                    vertx.eventBus().request("redis.set", payload, reply -> {
                        if (reply.succeeded()) {
                            logger.info(String.format(
                                    logFormat,
                                    LocalDateTime.now(),
                                    currentThread(),
                                    "SET OK " + uuid
                            ));
                            promise.complete(); // mark done
                        } else {
                            logger.error(String.format(
                                    logFormat,
                                    LocalDateTime.now(),
                                    currentThread(),
                                    "SET ERROR " + uuid
                            ), reply.cause());
                            promise.fail(reply.cause());
                        }
                    });
                });
            }

            // Combine all futures
            CompositeFuture.all(futures).onComplete(ar -> {
                if (ar.succeeded()) {
                    ctx.response()
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("status", "All 20 redis sets completed").toBuffer());
                } else {
                    ctx.response()
                            .setStatusCode(500)
                            .end(new JsonObject().put("error", ar.cause().getMessage()).toBuffer());
                }
            });
        });


        vertx.createHttpServer()
                .requestHandler(router)
                .listen(8888, http -> {
                    if (http.succeeded()) {
                        logger.info("HTTP server started on port 8888");
                        startPromise.complete();
                    } else {
                        startPromise.fail(http.cause());
                    }
                });
    }

    private String currentThread() {
        return Thread.currentThread().getName();
    }

    private int random() {
        return new Random().nextInt(5) + 1;
    }

    @Override
    public void stop() {
        logger.info("MainVerticle is stopping...");
    }


    public static void main(String[] args) {
        VertxOptions options = new VertxOptions()
                .setEventLoopPoolSize(2)
                .setWorkerPoolSize(50);
        Vertx vertx = Vertx.vertx(options);

        vertx.deployVerticle("org.duydio.RedisVerticle", new DeploymentOptions().setInstances(4), res -> {
            if (res.succeeded()) {
                System.out.println("Deployed 4 instances of RedisVerticle");

                vertx.deployVerticle(new MainVerticle(options), mainRes -> {
                    if (mainRes.succeeded()) {
                        System.out.println("MainVerticle started");
                    } else {
                        mainRes.cause().printStackTrace();
                    }
                });
            } else {
                res.cause().printStackTrace();
            }
        });
    }

}
