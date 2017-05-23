package org.embulk.plugin.common;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class DefaultColumnVisitor
    implements ColumnVisitor
{
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;

    public DefaultColumnVisitor(PageReader pageReader, PageBuilder pageBuilder)
    {
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setBoolean(column, pageReader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setLong(column, pageReader.getLong(column));
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setDouble(column, pageReader.getDouble(column));

    }

    @Override
    public void stringColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setString(column, pageReader.getString(column));
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
            return;
        }
        pageBuilder.setJson(column, pageReader.getJson(column));
    }
}
