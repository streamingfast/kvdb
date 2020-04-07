## Notes

About `meta:written`

A transaction row is composed of a multitude of family:column cells. Those cells
are stored through various for loops, each time accumulating the various cells
based on the row key. Normally, the values are flushed once all cells are available.

However, due to how the flushing occurs, it's possible that only 1/N cells are
currently in the pending mutations sets when the flush occurs.

This could lead to inconsistencies at retrieval time since there could be missing
cells when retrieving the full row in between flush.

The `meta:written` is used has a mechanism to inform the consumer, i.e. the one
reading the row, that it was not fully written yet and there is some cells
that will be appended in the database at a later time.

The `meta:written` is written as the very last operation of the storing
loop, ensuring that any previous cells values were written correctly.

The stitcher for example can use the `meta:written` cell to ignore those
rows resulting in the later run to get an nil transaction.
