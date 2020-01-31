package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    int numCols;
    int numRows;
    protected ByteBuffer rows;
    long[] rowSumCache;
    long col0Cache;
    protected ByteBuffer columns;

    public CustomTable() {
    }

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
        rowSumCache = new long[numRows];
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            long rowSum = 0l;
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int rowOffset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int columnOffset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                int fieldValue = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.columns.putInt(columnOffset, curRow.getInt(ByteFormat.FIELD_LEN*colId));
                this.rows.putInt(rowOffset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
                rowSum += fieldValue;
                if (colId == 0)
                    col0Cache += fieldValue;
            }
            rowSumCache[rowId] = rowSum;
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int columnOffset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columns.getInt(columnOffset);
    }

    private int getRowIntField(int rowId, int colId) {
        int rowOffset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rows.getInt(rowOffset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
//        irt.putIntField(rowId, colId, field);
        int rowOffset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int columnOffset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        int oldValue = rows.getInt(rowOffset);
        if (oldValue == field)
            return;
        if (colId == 0) {
            col0Cache -= oldValue;
            col0Cache += field;
        }
        long rowSumMapValue = rowSumCache[rowId];
        rowSumMapValue -= oldValue;
        rowSumMapValue += field;
        rowSumCache[rowId] = rowSumMapValue;
        rows.putInt(rowOffset, field);
        columns.putInt(columnOffset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
//        return irt.columnSum();
        return col0Cache;
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
                predicatedColumnSum += col0Field;
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
            int col0Value = getIntField(rowId, 0);
            if (col0Value > threshold) {
                predicatedAllColumnsSum += rowSumCache[rowId];
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
//        return irt.predicatedUpdate(threshold);
        int countUpdatedRows = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int col0Field = getIntField(rowId, 0);
            if (col0Field < threshold) {
                int col2Field = getRowIntField(rowId, 2);
                int col3Field = getRowIntField(rowId, 3);
                int updateField = col3Field + col2Field;
                putIntField(rowId,3, updateField);
                countUpdatedRows++;
            }
        }
        return countUpdatedRows;
    }

}
