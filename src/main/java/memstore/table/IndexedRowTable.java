package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
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
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        index = new TreeMap<>();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int fieldValue = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, fieldValue);
                if (colId == indexColumn) {
                    if (!index.containsKey(fieldValue)) {
                        index.put(fieldValue, new IntArrayList());
                    }
                    IntArrayList valueList = index.get(fieldValue);
                    valueList.add(rowId);
                }
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

    private void removeFromIndex(int rowId, int colId) {
        int oldValue = getIntField(rowId, colId);
        if (index.containsKey(oldValue)) {
            IntArrayList rowList = index.get(oldValue);
            rowList.rem(rowId);
            if (rowList.isEmpty())
                index.remove(oldValue);
        }
    }

    private void addToIndex(int rowId, int colId, int field) {
        if (!index.containsKey(field)) {
            index.put(field, new IntArrayList());
        }
        IntArrayList rowList = index.get(field);
        rowList.add(field);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        if (rowId < 0 || rowId >= numRows || colId < 0 || colId >= numCols)
            return;
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        rows.putInt(offset, field);
        if (indexColumn == colId) {
            removeFromIndex(rowId, colId);
            addToIndex(rowId, colId, field);
        }
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
            columnSum += getIntField(rowId, 0);
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
        Set<Integer> filteredRows = new HashSet<>();
        long predicatedColumnSum = 0l;
        if (indexColumn == 1) {
            SortedMap<Integer, IntArrayList> greaterMap = index.tailMap(threshold1 + 1);
            for (Map.Entry<Integer, IntArrayList> entry : greaterMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
        } else if (indexColumn == 2) {
            SortedMap<Integer, IntArrayList> lowerMap = index.headMap(threshold2);
            for (Map.Entry<Integer, IntArrayList> entry : lowerMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                filteredRows.add(rowId);
            }
        }
        for (int rowId : filteredRows) {
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
        Set<Integer> filteredRows = new HashSet<>();
        if (indexColumn == 0) {
            SortedMap<Integer, IntArrayList> greaterMap = index.tailMap(threshold + 1);
            for (Map.Entry<Integer, IntArrayList> entry : greaterMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                filteredRows.add(rowId);
            }
        }
        for (int rowId : filteredRows) {
            long rowSum = (long)getIntField(rowId, 0);
            if (indexColumn == 0 || rowSum > threshold) {
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
        Set<Integer> filteredRows = new HashSet<>();
        if (indexColumn == 0) {
            SortedMap<Integer, IntArrayList> greaterMap = index.headMap(threshold);
            for (Map.Entry<Integer, IntArrayList> entry : greaterMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                filteredRows.add(rowId);
            }
        }
        for (int rowId : filteredRows) {
            int col0Field = getIntField(rowId, 0);
            if (indexColumn == 0 || col0Field < threshold) {
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
