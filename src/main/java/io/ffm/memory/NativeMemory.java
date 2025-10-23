package io.ffm.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Author - Bhavya Joshi
 */
public final class NativeMemory implements AutoCloseable {

    private final Arena arena;
    private final MemorySegment memorySegment;
    private final boolean isMapped;
    private boolean closed = false;

    public NativeMemory(long size) {
        this.arena = Arena.ofConfined();
        this.memorySegment = arena.allocate(size);
        this.isMapped = false;
    }

    public NativeMemory(MemorySegment existing, Arena arena) {
        this.arena = arena;
        this.memorySegment = existing;
        this.isMapped = true;
    }


    /**
     * Memory Mapped I/O
     * Map data to memory segments
     * Uses same address space to address both CPU and I/O devices
     */
    public static NativeMemory mapFile(Path filePath) throws Exception {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("File too large: " + size + " bytes");
            }

            Arena arena = Arena.ofConfined();
            try {
                MemorySegment mappedSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
                return new NativeMemory(mappedSegment, arena);
            } catch (Exception e) {
                arena.close();
                throw e;
            }
        }
    }

    public MemorySegment getSegment() {
        if (closed) {
            throw new IllegalStateException("NativeMemory has been closed");
        }
        return memorySegment;
    }

    public long getSize() {
        return memorySegment.byteSize();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (arena.scope().isAlive()) {
                arena.close();
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            close();
        }
        super.finalize();
    }
}