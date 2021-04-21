package br.com.br.com.api;

import br.com.LocalDataBase;
import br.com.Order;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDataBase implements Closeable {
    private final LocalDataBase database;

    public OrdersDataBase() throws SQLException {
        this.database = new LocalDataBase("frauds_database");
        database.createIfNotExists(" create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean" +
                ")");
    }

    public boolean saveNew(Order order) throws SQLException {
        if(wasProcessed(order)){
            return false;
        }
        database.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    @Override
    public void close() throws IOException {
        database.close();
    }
}
