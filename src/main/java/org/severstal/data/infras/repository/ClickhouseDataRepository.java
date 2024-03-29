package org.severstal.data.infras.repository;

import org.severstal.data.domain.TenderItem;
import org.severstal.data.repository.DataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
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
}
