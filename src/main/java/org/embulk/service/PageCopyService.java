package org.embulk.service;

import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.slf4j.Logger;

public class PageCopyService
{
    private static final Logger logger = Exec.getLogger(PageCopyService.class);

    private PageCopyService()
    {
    }

    public static Page copy(Page page)
    {
        byte[] clone = page.buffer().array().clone();
        return Page.wrap(Buffer.wrap(clone, page.buffer().offset(), page.buffer().limit()))
                .setStringReferences(page.getStringReferences())
                .setValueReferences(page.getValueReferences());
    }
}
