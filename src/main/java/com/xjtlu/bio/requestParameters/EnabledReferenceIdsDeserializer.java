package com.xjtlu.bio.requestParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/** Prevent Jackson's numeric/string coercion from silently changing reference IDs. */
public final class EnabledReferenceIdsDeserializer extends JsonDeserializer<List<Long>> {

    @Override
    public List<Long> deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (!parser.isExpectedStartArrayToken()) {
            return context.reportInputMismatch(List.class, "enabledReferenceIds 必须为整数 ID 数组");
        }
        List<Long> ids = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() != JsonToken.VALUE_NUMBER_INT) {
                return context.reportInputMismatch(List.class, "enabledReferenceIds 中的每个 ID 必须为非空整数");
            }
            ids.add(parser.getLongValue());
        }
        return ids;
    }
}
