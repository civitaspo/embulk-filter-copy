package org.embulk.filter.copy.service;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;

public class OutForwardVisitor
    implements ColumnVisitor
{
    private final PageReader reader;
    private final OutForwardEventBuilder builder;

    public OutForwardVisitor(PageReader reader, OutForwardEventBuilder builder)
    {
        this.reader = reader;
        this.builder = builder;
    }

    private void nullOr(Column column, Runnable r)
    {
        if (reader.isNull(column)) {
            builder.setNull(column);
            return;
        }
        r.run();
    }

    @Override
    public void booleanColumn(Column column)
    {
        nullOr(column, () -> builder.setBoolean(column, reader.getBoolean(column)));
    }

    @Override
    public void longColumn(Column column)
    {
        nullOr(column, () -> builder.setLong(column, reader.getLong(column)));
    }

    @Override
    public void doubleColumn(Column column)
    {
        nullOr(column, () -> builder.setDouble(column, reader.getDouble(column)));
    }

    @Override
    public void stringColumn(Column column)
    {
        nullOr(column, () -> builder.setString(column, reader.getString(column)));
    }

    @Override
    public void timestampColumn(Column column)
    {
        nullOr(column, () -> builder.setTimestamp(column, reader.getTimestamp(column)));
    }

    @Override
    public void jsonColumn(Column column)
    {
        nullOr(column, () -> builder.setJson(column, reader.getJson(column)));
    }
}
