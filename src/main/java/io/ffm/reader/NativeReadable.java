package io.ffm.reader;

import io.ffm.memory.NativeMemory;
import org.apache.arrow.vector.VectorSchemaRoot;
import java.nio.file.Path;

/**
 * Author - Bhavya Joshi
 */
public interface NativeReadable {

    NativeMemory map(Path filePath) throws Exception;

    VectorSchemaRoot readToArrow(Path filePath) throws Exception;

}

