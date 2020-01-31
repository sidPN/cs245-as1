package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public RowTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long columnSum = 0L;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col0Value = getIntField(rowId, 0);
            columnSum += col0Value;
        }
        return columnSum;
    }


    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long predicatedColumnSum = 0l;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col1Field = getIntField(rowId, 1);
            int cold2Field = getIntField(rowId, 2);
            if (col1Field > threshold1 && cold2Field < threshold2) {
                int col0Field = getIntField(rowId, 0);
                predicatedColumnSum += (long) col0Field;
            }
        }
        return predicatedColumnSum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long predicatedAllColumnsSum = 0l;
        for (int rowId = 0; rowId < numRows; rowId++) {
            long rowSum = (long)getIntField(rowId, 0);
            if (rowSum > threshold) {
                for (int colId = 1; colId < numCols; colId++) {
                    rowSum += (long)getIntField(rowId, colId);
                }
                predicatedAllColumnsSum += rowSum;
            }
        }
        return predicatedAllColumnsSum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int countUpdatedRows = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col0Field = getIntField(rowId, 0);
            if (col0Field < threshold) {
                int col2Field = getIntField(rowId, 2);
                int col3Field = getIntField(rowId, 3);
                int updateField = col3Field + col2Field;
                putIntField(rowId,3, updateField);
                countUpdatedRows++;
            }
        }
        return countUpdatedRows;
    }
}
