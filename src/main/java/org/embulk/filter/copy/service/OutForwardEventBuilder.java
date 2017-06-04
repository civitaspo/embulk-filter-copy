package org.embulk.filter.copy.service;

import com.google.common.collect.Maps;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.value.Value;

import java.util.Map;

public class OutForwardEventBuilder
{
    private final Schema schema;
    private final TimestampFormatter timestampFormatter;

    private Map<String, Object> message;

    public OutForwardEventBuilder(
            Schema schema,
            TimestampFormatter timestampFormatter)
    {

        this.schema = schema;
        this.timestampFormatter = timestampFormatter;

        setNewMessage();
    }

    private void setNewMessage()
    {
        this.message = Maps.newHashMap();
    }

    public void emitMessage(OutForwardService outForward)
    {
        outForward.emit(message);
        setNewMessage();
    }

    public Schema getSchema()
    {
        return schema;
    }

    private Column getColumn(int columnIndex)
    {
        return getSchema().getColumn(columnIndex);
    }

    private void setValue(String columnName, Object value)
    {
        message.put(columnName, value);
    }

    private void setValue(Column column, Object value)
    {
        setValue(column.getName(), value);
    }

    public void setValue(int columnIndex, Object value)
    {
        setValue(getColumn(columnIndex), value);
    }

    public void setNull(Column column)
    {
        setValue(column, null);
    }

    private void setNull(int columnIndex)
    {
        setNull(getColumn(columnIndex));
    }

    public void setBoolean(Column column, boolean value)
    {
        setValue(column, value);
    }

    public void setBoolean(int columnIndex, boolean value)
    {
        setBoolean(getColumn(columnIndex), value);
    }

    public void setLong(Column column, long value)
    {
        setValue(column, value);
    }

    public void setLong(int columnIndex, long value)
    {
        setLong(getColumn(columnIndex), value);
    }

    public void setDouble(Column column, double value)
    {
        setValue(column, value);
    }

    public void setDouble(int columnIndex, double value)
    {
        setDouble(getColumn(columnIndex), value);
    }

    public void setString(Column column, String value)
    {
        setValue(column, value);
    }

    public void setString(int columnIndex, String value)
    {
        setString(getColumn(columnIndex), value);
    }

    public void setTimestamp(Column column, Timestamp value)
    {
        setValue(column, timestampFormatter.format(value));
    }

    public void setTimestamp(int columnIndex, Timestamp value)
    {
        setTimestamp(getColumn(columnIndex), value);
    }

    public void setJson(Column column, Value value)
    {
        setValue(column, value.toJson());
    }

    public void setJson(int columnIndex, Value value)
    {
        setJson(getColumn(columnIndex), value);
    }
}
