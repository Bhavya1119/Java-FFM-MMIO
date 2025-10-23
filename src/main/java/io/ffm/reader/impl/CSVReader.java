package io.ffm.reader.impl;

import io.ffm.memory.NativeMemory;
import io.ffm.reader.NativeReadable;
import io.ffm.reader.schema.ArrowSchema;
import io.ffm.reader.utils.SchemaUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Author - Bhavya Joshi
 */
public class CSVReader implements NativeReadable, AutoCloseable {

    private static final byte QUOTE = '"';
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final int FIELD_BUFFER_SIZE = 256;
    
    private final byte[] reusableBuffer = new byte[FIELD_BUFFER_SIZE];
    private final long[] reusablePosition = new long[1];
    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy")
    };

    private final byte delimiterByte;
    private final boolean hasHeader;
    private final int sampleSize;
    private final RootAllocator allocator;

    /**
     * Default constructor with comma delimiter, header enabled, and 10 sample rows for type inference.
     */
    public CSVReader() {
        this(",", true, 10);
    }

    /**
     * Constructs CSVReader with custom configuration.
     * @param delimiter Field separator character (e.g., ",", ";", "\t")
     * @param hasHeader Whether first row contains column headers
     * @param sampleSize Number of rows to sample for type inference
     */
    public CSVReader(String delimiter, boolean hasHeader, int sampleSize) {
        this.delimiterByte = delimiter.getBytes(StandardCharsets.UTF_8)[0];
        this.hasHeader = hasHeader;
        this.sampleSize = sampleSize;
        this.allocator = new RootAllocator();
    }

    /**
     * Maps CSV file to off-heap memory using memory-mapped I/O.
     * @param filePath Path to CSV file
     * @return NativeMemory wrapper containing memory-mapped file segment
     * @throws Exception If file mapping fails
     */
    @Override
    public NativeMemory map(Path filePath) throws Exception {
        return NativeMemory.mapFile(filePath);
    }

    /**
     * Reads CSV file and converts to Apache Arrow format with automatic schema inference.
     * Uses memory-mapped I/O for zero-copy reading and statistical type detection.
     * @param filePath Path to CSV file
     * @return VectorSchemaRoot containing parsed data in Arrow columnar format
     * @throws Exception If file reading or parsing fails
     */
    @Override
    public VectorSchemaRoot readToArrow(Path filePath) throws Exception {
        try (NativeMemory memory = map(filePath)) {
            MemorySegment segment = memory.getSegment();
            long size = memory.getSize();
            return parseWithSchemaInference(segment, size);
        }
    }

    /**
     * Core parsing method that infers schema and parses CSV data in a single pass.
     * Steps: 1) Infer schema from sample, 2) Allocate Arrow vectors, 3) Skip header, 4) Parse all rows
     * @param segment Memory-mapped file segment
     * @param size Total file size in bytes
     * @return VectorSchemaRoot with parsed data and inferred schema
     */
    private VectorSchemaRoot parseWithSchemaInference(MemorySegment segment, long size) {
        ArrowSchema schemaResult = SchemaUtils.inferSchema(segment, size, delimiterByte, sampleSize, hasHeader);

        Schema schema = schemaResult.schema();
        List<MinorType> types = schemaResult.types();

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        List<FieldVector> vectors = root.getFieldVectors();

        int estimatedRows = (int) Math.min(size / 50, 100000);
        for (FieldVector v : vectors) {
            v.setInitialCapacity(estimatedRows);
            v.allocateNew();
        }

        reusablePosition[0] = 0;
        if (hasHeader) {
            while (reusablePosition[0] < size && segment.get(ValueLayout.JAVA_BYTE, reusablePosition[0]) != LF) {
                reusablePosition[0]++;
            }
            if (reusablePosition[0] < size) reusablePosition[0]++;
        }

        int rowCount = parseAllRows(segment, size, reusablePosition, vectors, types);
        root.setRowCount(rowCount);
        return root;
    }

    /**
     * Parses all CSV rows from memory-mapped data into Arrow vectors.
     * Handles CSV quoting rules, field delimiters, and type-safe value conversion.
     * Uses reusable buffer to minimize allocations during parsing.
     * @param segment Memory segment containing CSV data
     * @param size Total data size
     * @param position Current reading position (modified during parsing)
     * @param vectors Arrow field vectors to populate
     * @param types Inferred column types for conversion
     * @return Total number of rows parsed
     */
    private int parseAllRows(MemorySegment segment, long size, long[] position, List<FieldVector> vectors, List<MinorType> types) {
        int rowCount = 0;
        int columnCount = vectors.size();

        while (position[0] < size) {
            int col = 0;
            int bufferPos = 0;
            boolean inQuotes = false;

            while (position[0] < size && col < columnCount) {
                byte b = segment.get(ValueLayout.JAVA_BYTE, position[0]);

                if (b == QUOTE) {
                    inQuotes = !inQuotes;
                    position[0]++;
                } else if (!inQuotes) {
                    if (b == delimiterByte) {
                        setVectorValue(vectors.get(col), types.get(col), rowCount, reusableBuffer, bufferPos);
                        bufferPos = 0;
                        col++;
                        position[0]++;
                    } else if (b == LF) {
                        setVectorValue(vectors.get(col), types.get(col), rowCount, reusableBuffer, bufferPos);
                        rowCount++;
                        position[0]++;
                        break;
                    } else if (b != CR) {
                        if (bufferPos < FIELD_BUFFER_SIZE) {
                            reusableBuffer[bufferPos++] = b;
                        }
                        position[0]++;
                    } else {
                        position[0]++;
                    }
                } else {
                    if (bufferPos < FIELD_BUFFER_SIZE) {
                        reusableBuffer[bufferPos++] = b;
                    }
                    position[0]++;
                }
            }

            if (position[0] >= size && col < columnCount && bufferPos > 0) {
                setVectorValue(vectors.get(col), types.get(col), rowCount, reusableBuffer, bufferPos);
                rowCount++;
            }
        }

        return rowCount;
    }

    /**
     * Sets typed value in Arrow vector based on inferred column type.
     * Performs type-specific parsing and handles conversion errors gracefully.
     * Falls back to VARCHAR on parse errors to maintain data integrity.
     * @param vector Target Arrow field vector
     * @param type Inferred column data type
     * @param row Row index to set
     * @param buffer Byte buffer containing field data
     * @param length Number of valid bytes in buffer
     */
    private void setVectorValue(FieldVector vector, MinorType type, int row, byte[] buffer, int length) {
        if (length == 0) {
            vector.setNull(row);
            return;
        }

        try {
            switch (type) {
                case INT -> ((IntVector) vector).setSafe(row, (int) parseNumber(buffer, length));
                case BIGINT -> ((BigIntVector) vector).setSafe(row, parseNumber(buffer, length));
                case FLOAT8 -> ((Float8Vector) vector).setSafe(row, parseDouble(buffer, length));
                case BIT -> ((BitVector) vector).setSafe(row, parseBoolean(buffer, length) ? 1 : 0);
                case DATEDAY -> ((DateDayVector) vector).setSafe(row, (int) parseDate(buffer, length).toEpochDay());
                default -> ((VarCharVector) vector).setSafe(row, buffer, 0, length);
            }
        } catch (Exception e) {
            try {
                ((VarCharVector) vector).setSafe(row, buffer, 0, length);
            } catch (Exception ex) {
                vector.setNull(row);
            }
        }
    }


    private long parseNumber(byte[] buffer, int length) {
        long result = 0;
        boolean negative = false;
        int i = 0;

        while (i < length && buffer[i] == ' ') i++;
        if (i < length && buffer[i] == '-') {
            negative = true;
            i++;
        } else if (i < length && buffer[i] == '+') {
            i++;
        }
        while (i < length && buffer[i] >= '0' && buffer[i] <= '9') {
            result = result * 10 + (buffer[i] - '0');
            i++;
        }
        return negative ? -result : result;
    }


    private double parseDouble(byte[] buffer, int length) {
        return Double.parseDouble(new String(buffer, 0, length, StandardCharsets.UTF_8).trim());
    }


    private boolean parseBoolean(byte[] buffer, int length) {
        if (length == 1) {
            byte b = buffer[0];
            return b == '1' || b == 'y' || b == 'Y' || b == 't' || b == 'T';
        }
        return "true".equalsIgnoreCase(new String(buffer, 0, length, StandardCharsets.UTF_8).trim());
    }


    private LocalDate parseDate(byte[] buffer, int length) {
        if (length == 10 && buffer[4] == '-' && buffer[7] == '-') {
            int year = (buffer[0] - '0') * 1000 + (buffer[1] - '0') * 100 + (buffer[2] - '0') * 10 + (buffer[3] - '0');
            int month = (buffer[5] - '0') * 10 + (buffer[6] - '0');
            int day = (buffer[8] - '0') * 10 + (buffer[9] - '0');
            return LocalDate.of(year, month, day);
        }
        
        String dateStr = new String(buffer, 0, length, StandardCharsets.UTF_8).trim();
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDate.parse(dateStr, formatter);
            } catch (Exception ignored) {}
        }
        throw new IllegalArgumentException("Unable to parse date: " + dateStr);
    }


    @Override
    public void close() {
        if (allocator != null) {
            allocator.close();
        }
    }
}