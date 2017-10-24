package org.embulk.filter.copy.spi;

import org.embulk.spi.Schema;

public class PageReader
        extends org.embulk.spi.PageReader
        implements DataReader
{
    public PageReader(Schema schema)
    {
        super(schema);
    }
}
