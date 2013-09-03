/**
 * 
 */
package com.malabarba.hugesqlitecursor;

import java.util.ArrayList;

import android.annotation.TargetApi;
import android.content.ContentResolver;
import android.database.CharArrayBuffer;
import android.database.ContentObserver;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.text.TextUtils;
import android.util.Log;

/**
 * @author artur
 *
 */
public class HugeSQLiteCursor implements Cursor {
    private final static String COUNT_COLUMN_NAME = "Count_01lc8a182nd110masdlk5";
    private final static String TAG = "HugeSQLiteCursor";
    
    private int mIdColumn = -1;
    private int mCount = -1;
    private int partialCount = -1;         // Patial Count
    
    private String mTable;
    private String[] mColumns;
    private String mWhere;
    private String[] mSearchText;
    private int mStep;
    private Cursor mCursor;
    private ArrayList<Cursor> mCursors = new ArrayList<Cursor> ();
    private SQLiteDatabase db;
//    private boolean  = false;
    
    // public HugeSQLiteCursor(SQLiteDatabase db, String table, String[] columns, String where, String[] searchText, String step, int size) {
    //     this(db, table, columns, where, searchText, step) ;
    //     mCount = size;
    // }
    
    /**
     * Creates an instance of HugeSQLiteCursor which is populated as
     * necessary with the query results. This means that queries with
     * huge results won't take long to load, in fact they'll take
     * almost exactly as long as a quqery with {@link step} results.
     * 
     * The cursor created should behave identically to the one you'd
     * get by running db.query(table, columns, selection,
     * selectionArgs, null, null, "_id", null).
     *
     * In other words, this constructor is almost identical to a
     * query(...) command, except results will be sorted by the "_id"
     * column (this is necessary for HugeSQLiteCursor to work) and
     * will be loaded only on demand. This also has the consequence
     * that if the database changes before the cursor has been fully
     * loaded, then further loaded results will take into accout the
     * new database.
     *
     * @param db The SQLiteDatabase to use for the query.
     * @param step The cursor will be populated in increments of size
     *            {@link step}. This should be invisible to the cursor
     *            adapter being used, as further increments are
     *            automatically loaded as necessary. The value
     *            directly decides the initial load time of the
     *            cursor: small values will initialize faster, but
     *            will need to perform more frequent re-queries as the
     *            user scrolls down the list (though you can manually
     *            increase the "buffer" by using loadUpTo(...)).
     *            Either way, this should never be less then the
     *            number of items which fit on the screen, or more
     *            then one query will be immediately necessary,
     *            slowing down initialization.
     * @param table The table name to compile the query against.
     * @param columns A list of which columns to return. Passing null
     *            will return all columns, which is discouraged to
     *            prevent reading data from storage that isn't going
     *            to be used. If neither a "*" nor a "_id" column is
     *            present, then a "_id" column will be added at the
     *            end.
     * @param selection A filter declaring which rows to return, formatted as an
     *            SQL WHERE clause (excluding the WHERE itself). Passing null
     *            will return all rows for the given table.
     * @param selectionArgs You may include ?s in selection, which
     *            will be replaced by the values from selectionArgs,
     *            in order that they appear in the selection. The
     *            values will be bound as Strings.
     * @return A {@link HugeSQLiteCursor} object.
     * @see SQLiteCursor
     */
    public HugeSQLiteCursor(SQLiteDatabase db, String step, String table, String[] columns, String selection, String[] searchText) {
        this.db = db;
        mTable = table;
        mWhere = selection;
        mSearchText = searchText;
        mStep = Integer.parseInt(step);

        if (columns == null) {
            mIdColumn = -2;
            // throw new IllegalArgumentException("columns can't be null "
            //                                    +"(I need to know selection _id column is)");
            
        } else {
            // Find the _id column, if it exists
            final int length = columns.length;
            for (int i = 0; i < length; i++) {
                // if (columns[i] == null)
                //     throw new IllegalArgumentException("columns can't contain null before _id "
                //                                        +"(I need to know selection _id column is)");
                if ("*".equals(columns[i])) mIdColumn = -2;
                // throw new IllegalArgumentException("columns can't contain * before _id "
                //                                    +"(I need to know selection _id column is)");
                else if ("_id".equals(columns[i])) mIdColumn = i;
            }           
        }
        // Add the _id column, if it doesn't exist (-1 means neither _id nor * were found)
        if (mIdColumn == -1) {
            mIdColumn = columns.length;
            mColumns = new String[mIdColumn + 1];
            if (mIdColumn > 0) System.arraycopy(columns, 0, mColumns, 0, mIdColumn);
            mColumns[mIdColumn] = "_id";    
        } else
            mColumns = columns;
        
        // Write the columns request for the cursor
        String concatColumns = "";
        if (mColumns == null) concatColumns = "*,";
        else for(String str: mColumns)
                 if (str != null) concatColumns += str + ", ";
        concatColumns += "(SELECT Count(*) FROM " + mTable + whereOrEmpty() + ") as "+COUNT_COLUMN_NAME;

        // We have doubled the number of ?s so we need to double the search text
        String[] doubleSearchText = null;
        if (mSearchText != null) {
            int size = mSearchText.length;
            doubleSearchText = new String[2 * size];
            System.arraycopy(mSearchText, 0, doubleSearchText, 0,    size);
            System.arraycopy(mSearchText, 0, doubleSearchText, size, size);
        }
        
        // Make the raw query and add it straight away
        mCursors.add(db.rawQuery("SELECT "+ concatColumns + " FROM " + ((mTable == null)?"":mTable)
                                 + whereOrEmpty() + " ORDER BY _id LIMIT " + step,
                                 doubleSearchText));
        
        // mCursors.add(db.query(mTable, mColumns, mWhere, mSearchText, null, null, "_id", step));
        mCursor = new MergeCursor(mCursors.toArray(new Cursor[mCursors.size()]));
    }

    private String whereOrEmpty() {return ((TextUtils.isEmpty(mWhere))? "" : " where " + mWhere);}
    
    private int queryCount(String[] searchText) {
        final int old = mCursor.getPosition();
        mCursor.moveToFirst();
        final int count = mCursor.getInt(mCursor.getColumnIndex(COUNT_COLUMN_NAME));
        mCursor.moveToPosition(old);
        return count;
    }
    
    
    public int getPartialCount() {
        if (partialCount < 0) {
            if (mIdColumn < 0)
                mIdColumn = mCursor.getColumnIndex("_id");
            partialCount = mCursor.getCount();
            if (Log.isLoggable(TAG, Log.DEBUG)) Log.d(TAG,"Counted Partial Size ("+partialCount+")");
        }
        return partialCount;
    }
    
    /* (non-Javadoc)
     * @see android.database.Cursor#getCount()
     */
    @Override
    public int getCount() {
        if (mCount < 0) {
            mCount = (getPartialCount() > 0)? queryCount(mSearchText) : 0;
            if (Log.isLoggable(TAG, Log.DEBUG)) Log.d(TAG,"Counted Full Size ("+mCount+")");
        }
        return mCount;
    }

    /* (non-Javadoc)
     * @see android.database.Cursor#moveToPosition(int)
     */
@Override
    public boolean moveToPosition(int arg0) {
        loadUpTo(arg0+mStep);
        if (Log.isLoggable(TAG, Log.VERBOSE)) Log.v(TAG,"Moving to "+arg0);
        
        return mCursor.moveToPosition(arg0);
    }

    /**
     * Make sure the cursor is loaded at least up to position pos, but it could be a lot more.
     *
     * This allows you to manually "buffer" the cursor after it has
     * finished initializing. For instance, calling
     * cursor.loadUpTo(cursor.getCount()) loads the entire cursor.
     * This could be called once the app is not doing any work so that
     * items don't have to be loaded real-time when the user starts
     * scrolling (useful for slower systems). It can also be used if
     * you'd like to finalize the cursor because you're about to
     * change something in the database.
     *
     * @param pos The minimum position you want to be loaded.
     * @return void
     */
    public void loadUpTo(int last) {
        int pCount = getPartialCount();
        if ((pCount < getCount()) && (last >= pCount)) { //isFinished
            
            if (Log.isLoggable(TAG, Log.DEBUG)) Log.d(TAG,"Loading HugeSQLiteCursor up to ["+last+"] (currently it's "+(pCount-1)+").");
        
            // Save the position for latter
            final int oldPos = mCursor.getPosition();

            // Get the last _id we have
            mCursor.moveToLast();
            final int lastId = mCursor.getInt(mIdColumn);

            // How Many steps to we need to add?
            final int times = (last + 1 - pCount)/mStep +
                (((last + 1 - pCount) % mStep) == 0 ? 1 : 2); // 1 more then necessary, for safety.

            Cursor newCursor = db.query(mTable, mColumns,
                                        ((mWhere == null)? "": mWhere  + " AND ") + "_id > " + lastId,
                                        mSearchText, null, null,
                                        "_id", Integer.toString(times*mStep));
            // isFinished = (newCursor.getCount() < times*mStep);
            
            mCursors.add(newCursor);
        
            mCursor = new MergeCursor(mCursors.toArray(new Cursor[mCursors.size()]));
        
            partialCount = mCursor.getCount();
            mCursor.moveToPosition(oldPos);
            if (Log.isLoggable(TAG, Log.DEBUG)) Log.d(TAG,"Loaded HugeSQLiteCursor up to ["+(partialCount-1)+"].");
        }
    }

    /* (non-Javadoc)
     * @see android.database.Cursor#isLast()
     */
    @Override
    public boolean isLast() {
        int cnt = getCount();
        return ((getPosition() == (cnt - 1))) && (cnt != 0); //&& isFinished;
    }
    /* (non-Javadoc)
     * @see android.database.Cursor#isAfterLast()
     */
    @Override
    public final boolean isAfterLast() {
        return (getCount() == 0) || (getPosition() == getCount());
    }

    // Methods that don't change
        
    /* (non-Javadoc)
     * @see android.database.Cursor#close()
     */
    @Override
    public void close() {mCursor.close();}
    /* (non-Javadoc)
     * @see android.database.Cursor#copyStringToBuffer(int, android.database.CharArrayBuffer)
     */
    @Override
    public void copyStringToBuffer(int arg0, CharArrayBuffer arg1) {mCursor.copyStringToBuffer(arg0, arg1);}
    /* (non-Javadoc)
     * @see android.database.Cursor#deactivate()
     */
    @Override
    @Deprecated
    public void deactivate() {mCursor.deactivate();}
    /* (non-Javadoc)
     * @see android.database.Cursor#getBlob(int)
     */
    @Override
    public byte[] getBlob(int arg0) {return mCursor.getBlob(arg0);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getColumnCount()
     */
    @Override
    public int getColumnCount() {return mCursor.getColumnCount();}
    /* (non-Javadoc)
     * @see android.database.Cursor#getColumnIndex(java.lang.String)
     */
    @Override
    public int getColumnIndex(String columnName) {return mCursor.getColumnIndex(columnName);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getColumnIndexOrThrow(java.lang.String)
     */
    @Override
    public int getColumnIndexOrThrow(String columnName)
        throws IllegalArgumentException {return mCursor.getColumnIndexOrThrow(columnName);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getColumnName(int)
     */
    @Override
    public String getColumnName(int columnIndex) {return mCursor.getColumnName(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getColumnNames()
     */
    @Override
    public String[] getColumnNames() {return mCursor.getColumnNames();}
    /* (non-Javadoc)
     * @see android.database.Cursor#getDouble(int)
     */
    @Override
    public double getDouble(int columnIndex) {return mCursor.getDouble(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getExtras()
     */
    @Override
    public Bundle getExtras() {return mCursor.getExtras();}
    /* (non-Javadoc)
     * @see android.database.Cursor#getFloat(int)
     */
    @Override
    public float getFloat(int columnIndex) {return mCursor.getFloat(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getInt(int)
     */
    @Override
    public int getInt(int columnIndex) {return mCursor.getInt(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getLong(int)
     */
    @Override
    public long getLong(int columnIndex) {return mCursor.getLong(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getPosition()
     */
    @Override
    public int getPosition() {return mCursor.getPosition();}
    /* (non-Javadoc)
     * @see android.database.Cursor#getShort(int)
     */
    @Override
    public short getShort(int columnIndex) {return mCursor.getShort(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getString(int)
     */
    @Override
    public String getString(int columnIndex) {return mCursor.getString(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getType(int)
     */
    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    @Override
    public int getType(int columnIndex) {return mCursor.getType(columnIndex);}
    /* (non-Javadoc)
     * @see android.database.Cursor#getWantsAllOnMoveCalls()
     */
    @Override
    public boolean getWantsAllOnMoveCalls() {return mCursor.getWantsAllOnMoveCalls();}
    /* (non-Javadoc)
     * @see android.database.Cursor#isBeforeFirst()
     */
    @Override
    public boolean isBeforeFirst() {return mCursor.isBeforeFirst();}

    /* (non-Javadoc)
     * @see android.database.Cursor#isClosed()
     */
    @Override
    public boolean isClosed() {return mCursor.isClosed();}

    /* (non-Javadoc)
     * @see android.database.Cursor#isFirst()
     */
    @Override
    public boolean isFirst() {return mCursor.isFirst();}

    /* (non-Javadoc)
     * @see android.database.Cursor#isNull(int)
     */
    @Override
    public boolean isNull(int columnIndex) {return mCursor.isNull(columnIndex);}

    // /* (non-Javadoc)
    //  * @see android.database.Cursor#move()
    //  */
    // @Override
    // public final boolean move(int offset) {return moveToPosition(getPosition() + offset);}
    // /* (non-Javadoc)
    //  * @see android.database.Cursor#moveToFirst()
    //  */
    // @Override
    // public final boolean moveToFirst() {return moveToPosition(0);}
    // /* (non-Javadoc)
    //  * @see android.database.Cursor#moveToLast()
    //  */
    // @Override
    // public final boolean moveToLast() {return moveToPosition(getCount() - 1);}
    // /* (non-Javadoc)
    //  * @see android.database.Cursor#moveToNext()
    //  */
    // @Override
    // public final boolean moveToNext() {return moveToPosition(getPosition() + 1);}
    // /* (non-Javadoc)
    //  * @see android.database.Cursor#moveToPrevious()
    //  */
    @Override
    public final boolean moveToPrevious() {return moveToPosition(getPosition() - 1);}
    /* (non-Javadoc)
     * @see android.database.Cursor#move(int)
     */
    @Override
    public boolean move(int offset) {return moveToPosition(getPosition() + offset);}

    /* (non-Javadoc)
     * @see android.database.Cursor#moveToFirst()
     */
    @Override
    public boolean moveToFirst() {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see android.database.Cursor#moveToLast()
     */
    @Override
    public boolean moveToLast() {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see android.database.Cursor#moveToNext()
     */
    @Override
    public boolean moveToNext() {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see android.database.Cursor#registerContentObserver(android.database.ContentObserver)
     */
        @Override
    public void registerContentObserver(ContentObserver arg0) {mCursor.registerContentObserver(arg0);}

    /* (non-Javadoc)
     * @see android.database.Cursor#registerDataSetObserver(android.database.DataSetObserver)
     */
        @Override
    public void registerDataSetObserver(DataSetObserver arg0) {mCursor.registerDataSetObserver(arg0);}

    /* (non-Javadoc)
     * @see android.database.Cursor#requery()
     */
    @Override
    @Deprecated
    public boolean requery() {return mCursor.requery();}

    /* (non-Javadoc)
     * @see android.database.Cursor#respond(android.os.Bundle)
     */
    @Override
    public Bundle respond(Bundle arg0) {return mCursor.respond(arg0);}

    /* (non-Javadoc)
     * @see android.database.Cursor#setNotificationUri(android.content.ContentResolver, android.net.Uri)
     */
    @Override
    public void setNotificationUri(ContentResolver arg0, Uri arg1) {mCursor.setNotificationUri(arg0, arg1);}

    /* (non-Javadoc)
     * @see android.database.Cursor#unregisterContentObserver(android.database.ContentObserver)
     */
    @Override
    public void unregisterContentObserver(ContentObserver arg0) {mCursor.unregisterContentObserver(arg0);}

    /* (non-Javadoc)
     * @see android.database.Cursor#unregisterDataSetObserver(android.database.DataSetObserver)
     */
    @Override
    public void unregisterDataSetObserver(DataSetObserver arg0) {mCursor.unregisterDataSetObserver(arg0);}

}
