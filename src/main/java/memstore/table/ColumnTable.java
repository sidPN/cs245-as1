package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.List;

/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 *   col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
    int numCols;
    int numRows;
    ByteBuffer columns;

    public ColumnTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                this.columns.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN*colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columns.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columns.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long columnSum = 0l;
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
        int[] col0Values = new int[numRows];

        for (int colId = 0; colId < numCols; colId++) {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (colId == 0) {
                    int col0Value = getIntField(rowId, 0);
                    col0Values[rowId] = col0Value;
                }
                if (col0Values[rowId] > threshold) {
                    if (colId == 0) {
                        predicatedAllColumnsSum += col0Values[rowId];
                    } else {
                        predicatedAllColumnsSum += getIntField(rowId, colId);
                    }
                }
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
