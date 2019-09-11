package edu.buffalo.cse.cse486586.simpledynamo;


import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class SQLiteHelper extends SQLiteOpenHelper {
    public static final String TABLE = "messages";
    public static final String COLUMN = "key";


    private static final String DATABASE_NAME = "simpledht.db";
    private static final int DATABASE_VERSION = 1;
    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + "messages";

    private static final String DATABASE_CREATE = "create table messages('key' text Primary key not null,value text not null,ver int not null)";

    public SQLiteHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        // db.execSQL(SQL_DELETE_ENTRIES);
        db.execSQL(DATABASE_CREATE);
        Log.v("query", "DB 44CREATED");
        Log.v("query", "df343df");

        Cursor dbCursor = db.query("messages", null, null, null, null, null, null);
        String[] columnNames = dbCursor.getColumnNames();
        Log.v("query", "dfdf");
        Log.v("query", columnNames+"");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
    public int getMaxKey(SQLiteDatabase db)
    {
        int id=0;
        String sql="Select max([key]) as [key] from "+ TABLE +";";
        db.execSQL(sql);
        Cursor c = db.rawQuery(sql, null);
        if (c.moveToFirst()) {
            id = c.getInt(c.getColumnIndex("id"));
        }
        c.close();
        return id;
    }
    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }
}

