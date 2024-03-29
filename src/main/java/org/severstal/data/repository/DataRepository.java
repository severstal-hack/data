package org.severstal.data.repository;

import org.severstal.data.domain.Product;
import org.severstal.data.domain.TenderItem;

import java.sql.SQLException;
import java.util.List;

public interface DataRepository {
    int AddParsedItems(List<TenderItem> items) throws SQLException;

    List<Product> GetProducts() throws SQLException;

    List<TenderItem> Match(String phrase) throws SQLException;
}
