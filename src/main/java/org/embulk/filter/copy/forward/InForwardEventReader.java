package org.embulk.filter.copy.forward;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import influent.EventStream;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.msgpack.value.Value;

import java.util.Map;

public class InForwardEventReader
{
    private final Schema schema;
    private final Map<String, Column> columnMap;

    private final JsonParser jsonParser = new JsonParser();
    private final TimestampParser timestampParser;

    private EventStream event = null;
    private int eventMessageCount = 0;

    private int readCount = 0;
    private Map<String, Value> message = Maps.newHashMap();

    public InForwardEventReader(Schema schema, TimestampParser timestampParser)
    {
        this.schema = schema;
        ImmutableMap.Builder<String, Column> builder = ImmutableMap.builder();
        schema.getColumns().forEach(column -> builder.put(column.getName(), column));
        this.columnMap = builder.build();
        this.timestampParser = timestampParser;
    }

    public Schema getSchema()
    {
        return schema;
    }

    private Column getColumn(String columnName)
    {
        return columnMap.get(columnName);
    }

    private Column getColumn(int columnIndex)
    {
        return getSchema().getColumn(columnIndex);
    }

    private Value getValue(Column column)
    {
        return message.get(column.getName());
    }

    public void setEvent(EventStream event)
    {
        this.event = event;
        this.eventMessageCount = event.getEntries().size();
    }

    public boolean isNull(Column column)
    {
        return getValue(column).isNilValue();
    }

    public boolean isNull(int columnIndex)
    {
        return isNull(getColumn(columnIndex));
    }

    public boolean getBoolean(Column column)
    {
        return getValue(column).asBooleanValue().getBoolean();
    }

    public boolean getBoolean(int columnIndex)
    {
        return getBoolean(getColumn(columnIndex));
    }

    public long getLong(Column column)
    {
        return getValue(column).asIntegerValue().asLong();
    }

    public long getLong(int columnIndex)
    {
        return getLong(getColumn(columnIndex));
    }

    public double getDouble(Column column)
    {
        return getValue(column).asFloatValue().toDouble();
    }

    public double genDouble(int columnIndex)
    {
        return getDouble(getColumn(columnIndex));
    }

    public String getString(Column column)
    {
        return getValue(column).toString();
    }

    public String getString(int columnIndex)
    {
        return getString(getColumn(columnIndex));
    }

    public Timestamp getTimestamp(Column column)
    {
        return timestampParser.parse(getString(column));
    }

    public Timestamp getTimestamp(int columnIndex)
    {
        return getTimestamp(getColumn(columnIndex));
    }

    public Value getJson(Column column)
    {
        return jsonParser.parse(getString(column));
    }

    public Value getJson(int columnIndex)
    {
        return getJson(getColumn(columnIndex));
    }

    public boolean nextMessage()
    {
        if (eventMessageCount <= readCount) {
            return false;
        }

        ImmutableMap.Builder<String, Value> builder = ImmutableMap.builder();
        event.getEntries().get(readCount++).getRecord().entrySet().forEach(
                entry -> builder.put(entry.getKey().toString(), entry.getValue())
        );
        message = builder.build();
        return true;
    }
}
