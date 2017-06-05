package org.embulk.filter.copy.forward;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;

public class InForwardVisitor
    implements ColumnVisitor
{
    private final InForwardEventReader reader;
    private final PageBuilder builder;

    public InForwardVisitor(InForwardEventReader reader, PageBuilder builder)
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
