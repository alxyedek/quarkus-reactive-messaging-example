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

    private final long tickInterval;
    private final long processDelay;
    private final int numTicks;

    public GreetingResource() {
        this.numTicks = 5;
        // milliseconds level (<10ms) precision produce unpredicted order:
        //    eg: after processor gets the first message, the producer emmits/sends (almost?) all messages, (almost?) instantly !! 
        this.tickInterval = 10L;
        this.processDelay = 20L;
    }

    // Inject an Emitter to control the 'ticks' stream.
    @Inject
    @Channel("ticks")
    Emitter<Long> ticksEmitter;

    private String withThreadTimestamp(Object obj) {
        return Thread.currentThread().getName() + "_@_" + Instant.now().toEpochMilli()+"_=_" + obj.toString();
    }

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
            .ticks().every(Duration.ofMillis(this.tickInterval))
            .select().first(this.numTicks)
            .onItem().invoke(tick -> System.out.println("Emitted   before:\t " + withThreadTimestamp(tick)))
            // We now use a lambda expression here to perform an action (logging)
            // before the message is sent to the channel.
            .subscribe().with(tick -> {
                ticksEmitter.send(tick);
                System.out.println("Emitted   after:\t " + withThreadTimestamp(tick));
            });

        // This log statement, after the .subscribe() call, confirms that the stream
        // has been initiated.
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
            // Here we configure the overflow behavior. This stream will buffer up to 3 items.
            // When the buffer is full, it will start to apply backpressure to the producer.
            // .onOverflow().buffer(2)
            .onOverflow().drop()
            // Use call to perform a non-blocking operation that logs and then delays.
            .onItem().call(tick -> {
                System.out.println("Processed   before:\t " + withThreadTimestamp(tick));
                // Return a Uni that delays the stream for 3 milliseconds.
                return Uni.createFrom().item(tick).onItem().delayIt().by(Duration.ofMillis(processDelay));
            })
            .onItem().invoke(tick -> System.out.println("Processed   after:\t " + withThreadTimestamp(tick)) )
            // .map(tick -> tick)
            .map(tick -> "" + tick )
            ;
    }

    /**
     * Consumes the processed timestamps from the 'times' channel and prints them.
     * The blocking sleep has been removed here, as the delay is now handled upstream.
     */
    @Incoming("times")
    public void consumer(String tick) {
        // System.out.println("Before consuming\t " + payload);
        // payload
        // .onItem().invoke(i -> System.out.println("Message consumed\t on thread: " + Thread.currentThread().getName() + " with payload: " + i))
        // .subscribe()
        ;

        System.out.println("Consumed   after:\t " + withThreadTimestamp(tick));
    }
}
