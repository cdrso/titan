a rewrite will pretty much be required based on stuff we found when glueing together

we need to consider log buffers for the architecture to remove copies from the spsc data pipelines
we need to consider how retransmission logic will fit with the log buffers
we need to have packing for each dedicated endpoint available, with adaptive algo for balancing throughput and latency
we need to bypass the udp for localhost data transmission
we need to properly think through the serialization and fragmentation pipeline, maybe some way to direcly put types in the log buffer, as if it was just raw bytes
we need to consider the balance between faster serialization vs smaller packing, the bottleneck is likely the network not the cpu
we need to carefully design the d2d protocol
we need some more stuff, but at least now we have some trash that is able to ping and pong between clients each with its corresponding driver

