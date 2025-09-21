package org.acme;

import io.smallrye.mutiny.Multi;
import  org.eclipse.microprofile.reactive.messaging.Emitter;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.Channel;

import java.time.Duration;
import java.time.Instant;

/**
 * A REST resource that demonstrates triggering a Reactive Messaging stream
 * from a REST endpoint using an Emitter.
 */
@Path("/hello")
public class GreetingResource {

    // Inject a MutinyEmitter to control the 'ticks' stream.
    // The @Channel annotation is used to link the Emitter to a named channel.
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
     * Maps each tick to the current instant in string format.
     */
    @Incoming("ticks")
    @Outgoing("times")
    public Multi<String> processor(Multi<Long> ticks) {
        return ticks
            .onItem()
            .invoke(tick -> System.out.println("Before processing\t "+tick))
            .invoke(tick -> {
                try {
                    Thread.sleep(3L);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            })
            .invoke(tick -> System.out.println("Message processed\t on thread: " + Thread.currentThread().getName() + " with payload: " + tick))
            .map(tick -> ""+tick+"_"+Instant.now().toEpochMilli());
    }

    /**
     * Consumes the processed timestamps from the 'times' channel and prints them.
     */
    @Incoming("times")
    public void consumer(String payload) {
        System.out.println("Before consuming\t "+payload);
        try {
            Thread.sleep(4L);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Message consumed\t on thread: " + Thread.currentThread().getName() + " - " + payload);
    }
}
