package org.acme;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.time.Duration;
import java.time.Instant;

/**
 * A REST resource that demonstrates triggering a Reactive Messaging stream
 * from a REST endpoint using an Emitter.
 */
@Path("/hello")
public class GreetingResource {

    // Inject an Emitter to control the 'ticks' stream.
    @Inject
    @Channel("ticks")
    Emitter<Long> ticksEmitter;

    /**
     * This REST endpoint now triggers the message creation.
     * It uses the injected emitter to send ticks to the 'ticks' channel.
     * The method returns immediately, while the stream runs asynchronously.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        System.out.println("Hello endpoint called on thread: " + Thread.currentThread().getName());
        
        // Use a Mutiny Multi to generate the ticks, then send them to the emitter.
        // This keeps the logic for producing the ticks separate from the emitter itself.
        Multi.createFrom()
            .ticks().every(Duration.ofMillis(1))
            .select().first(5)
            .onItem().invoke(tick -> System.out.println("Message produced\t on thread: " + Thread.currentThread().getName() + " with payload: " + tick))
            .subscribe().with(ticksEmitter::send);

        System.out.println("Hello endpoint returned on thread: " + Thread.currentThread().getName());
        return "Hello from Quarkus REST. Check the console for timestamps!";
    }

    /**
     * Processes the incoming ticks from the 'ticks' channel.
     * This method now uses a non-blocking delay.
     */
    @Incoming("ticks")
    @Outgoing("times")
    public Multi<String> processor(Multi<Long> ticks) {
        return ticks
            // Use call to perform a non-blocking operation that logs and then delays.
            .onItem().call(tick -> {
                System.out.println("Before processing\t on thread: " + Thread.currentThread().getName() + " with payload: " + tick);
                // Return a Uni that delays the stream for 3 milliseconds.
                return Uni.createFrom().item(tick).onItem().delayIt().by(Duration.ofMillis(3));
            })
            .onItem().invoke(tick -> System.out.println("Message processed\t on thread: " + Thread.currentThread().getName() + " with payload: " + tick))
            .map(tick -> "" + tick + "_" + Instant.now().toEpochMilli());
    }

    /**
     * Consumes the processed timestamps from the 'times' channel and prints them.
     * The blocking sleep has been removed here, as the delay is now handled upstream.
     */
    @Incoming("times")
    public void consumer(String payload) {
        // System.out.println("Before consuming\t " + payload);
        System.out.println("Message consumed\t on thread: " + Thread.currentThread().getName() + " - " + payload);
    }
}
