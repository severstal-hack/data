package org.severstal.data.infras.repository;

import org.severstal.data.domain.Product;
import org.severstal.data.domain.TenderItem;
import org.severstal.data.repository.DataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component("clickhouse-repository")
public class ClickhouseDataRepository implements DataRepository {

    private final Connection conn;

    public ClickhouseDataRepository(@Autowired Connection conn) {
        this.conn = conn;
    }

    @Override
    public int AddParsedItems(List<TenderItem> items) throws SQLException {
        String query = "INSERT INTO parsed_items values (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = this.conn.prepareStatement(query)) {
            for (TenderItem item : items) {
                UUID uuid = UUID.randomUUID();
                ps.setObject(1, uuid, Types.OTHER);
                ps.setString(2, item.getLink());
                ps.setString(3, item.getName());
                ps.setDouble(4, item.getCount());
                ps.setString(5, item.getUnit());
                ps.setDouble(6, item.getPrice());
                ps.setString(7, item.getAddress());
                ps.addBatch();
            }
            return ps.executeBatch().length;
        }
    }

    @Override
    public List<Product> GetProducts() throws SQLException {
        String query = "select product_name from own_products";
        List<Product> products = new ArrayList<>();
        try (PreparedStatement ps = this.conn.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                var productName = rs.getString(1);
                var product = new Product();
                product.setName(productName);

                products.add(product);
            }

        }

        return products;
    }

    @Override
    public List<TenderItem> Match(String phrase) throws SQLException {
        String query = "select link, name, count, unit\n" +
                "from parsed_items\n" +
                "where (ngramDistanceCaseInsensitive(name, ?) as t) < 0.89\n" +
                "order by t asc;";
        List<TenderItem> items = new ArrayList<>();
        try (PreparedStatement ps = this.conn.prepareStatement(query)) {
            ps.setString(1, phrase);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                var link = rs.getString(1);
                var name = rs.getString(2);
                var count = rs.getDouble(3);
                var unit = rs.getString(4);

                items.add(new TenderItem(link, name, count, unit, 0, ""));
            }

        }

        return items;
    }
}
