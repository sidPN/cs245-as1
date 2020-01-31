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
    long[] rowSumCache;
    long col0Cache;

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
        rowSumCache = new long[numRows];
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        index = new TreeMap<>();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            long rowSum = 0l;
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
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rows.getInt(offset);
    }

    private void removeFromIndex(int rowId, int oldValue) {
        if (index.containsKey(oldValue)) {
            IntArrayList rowList = index.get(oldValue);
            rowList.rem(rowId);
            if (rowList.isEmpty())
                index.remove(oldValue);
        }
    }

    private void addToIndex(int rowId, int field) {
        if (!index.containsKey(field)) {
            index.put(field, new IntArrayList());
        }
        IntArrayList rowList = index.get(field);
        rowList.add(rowId);
    }

    private void updateRowSumMap(int rowId, int oldValue, int newValue) {
        long rowSumMapValue = rowSumCache[rowId];
        rowSumMapValue -= oldValue;
        rowSumMapValue += newValue;
        rowSumCache[rowId] = rowSumMapValue;
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        if (rowId < 0 || rowId >= numRows || colId < 0 || colId >= numCols)
            return;
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int oldValue = rows.getInt(offset);
        if (oldValue == field)
            return;
        if (indexColumn == colId) {
            removeFromIndex(rowId, oldValue);
            addToIndex(rowId, field);
        }
        if (colId == 0) {
            col0Cache -= oldValue;
            col0Cache += field;
        }
        //updateRowSumMap(rowId, oldValue, field);
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
        return col0Cache;
    }

    private int getPredicatedColumnValue(int rowId, int threshold1, int threshold2) {
        int col1Field = getIntField(rowId, 1);
        int cold2Field = getIntField(rowId, 2);
        if (col1Field > threshold1 && cold2Field < threshold2) {
            return getIntField(rowId, 0);
        }
        return 0;
    }

    private long getPredicatedColumnSum(Set<Integer> filteredRows, int threshold1, int threshold2) {
        long predicatedColumnSum = 0l;
        if (!filteredRows.isEmpty()) {
            for (int rowId : filteredRows) {
                predicatedColumnSum += getPredicatedColumnValue(rowId, threshold1, threshold2);
            }
            return predicatedColumnSum;
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                predicatedColumnSum += getPredicatedColumnValue(rowId, threshold1, threshold2);;
            }
            return predicatedColumnSum;
        }
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
        if (indexColumn == 1) {
            SortedMap<Integer, IntArrayList> greaterMap = index.tailMap(threshold1, false);
            for (Map.Entry<Integer, IntArrayList> entry : greaterMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
            return getPredicatedColumnSum(filteredRows, threshold1, threshold2);
        } else if (indexColumn == 2) {
            SortedMap<Integer, IntArrayList> lowerMap = index.headMap(threshold2, false);
            for (Map.Entry<Integer, IntArrayList> entry : lowerMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
            return getPredicatedColumnSum(filteredRows, threshold1, threshold2);
        } else {
                return getPredicatedColumnSum(filteredRows, threshold1, threshold2);
        }
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

//        for (int rowId = 0; rowId < numRows; rowId++) {
//            int col0Value = getIntField(rowId, 0);
//            if (col0Value > threshold) {
//                predicatedAllColumnsSum += rowSumCache[rowId];
//            }
//        }
        for (int rowId = 0; rowId < numRows; rowId++) {
            long rowSum = getIntField(rowId, 0);
            if (rowSum > threshold) {
                for (int colId = 1; colId < numCols; colId++) {
                    rowSum += getIntField(rowId, colId);
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
            SortedMap<Integer, IntArrayList> lowerMap = index.headMap(threshold, false);
            for (Map.Entry<Integer, IntArrayList> entry : lowerMap.entrySet()) {
                filteredRows.addAll(entry.getValue());
            }
            for (int rowId : filteredRows) {
                predicatedUpdateHelper(rowId);
                countUpdatedRows++;
            }
            return countUpdatedRows;
        } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                int col0Value = getIntField(rowId, 0);
                if (col0Value < threshold) {
                    predicatedUpdateHelper(rowId);
                    countUpdatedRows++;
                }
            }
            return countUpdatedRows;
        }
    }

    private void predicatedUpdateHelper(int rowId) {
        int col2Field = getIntField(rowId, 2);
        int col3Field = getIntField(rowId, 3);
        int updateField = col3Field + col2Field;
        putIntField(rowId, 3, updateField);
    }
}
