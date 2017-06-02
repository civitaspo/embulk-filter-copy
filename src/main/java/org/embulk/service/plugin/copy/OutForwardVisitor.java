package org.embulk.service.plugin.copy;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import java.util.Map;

public class OutForwardVisitor
    implements ColumnVisitor
{
    private final PageReader reader;
    private final TimestampFormatter timestampFormatter;
    private Map<String, Object> message;

    public OutForwardVisitor(PageReader reader, TimestampFormatter timestampFormatter)
    {
        this.reader = reader;
        this.timestampFormatter = timestampFormatter;
    }
    
    public void setMessage(Map<String, Object> message)
    {
        this.message = message;
    }

    private void nullOr(Column column, Runnable runnable)
    {
        if (reader.isNull(column)) {
            message.put(column.getName(), null);
            return;
        }
        runnable.run();
    }

    @Override
    public void booleanColumn(Column column)
    {
        nullOr(column, () -> message.put(column.getName(), reader.getBoolean(column)));
    }

    @Override
    public void longColumn(Column column)
    {
        nullOr(column, () -> message.put(column.getName(), reader.getLong(column)));
    }

    @Override
    public void doubleColumn(Column column)
    {
        nullOr(column, () -> message.put(column.getName(), reader.getDouble(column)));
    }

    @Override
    public void stringColumn(Column column)
    {
        nullOr(column, () -> message.put(column.getName(), reader.getString(column)));
    }

    @Override
    public void timestampColumn(Column column)
    {
        nullOr(column, () -> message.put(column.getName(), timestampFormatter.format(reader.getTimestamp(column))));
    }

    @Override
    public void jsonColumn(Column column)
    {
        // TODO: explain why `toJson` is required.
        nullOr(column, () -> message.put(column.getName(), reader.getJson(column).toJson()));
    }
}
