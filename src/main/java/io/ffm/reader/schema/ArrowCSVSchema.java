package io.ffm.reader.schema;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Author - Bhavya Joshi
 */
public record ArrowCSVSchema(Schema schema, List<String> headers, List<Types.MinorType> types) { }
