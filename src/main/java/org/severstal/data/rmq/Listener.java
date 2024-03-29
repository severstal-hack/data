package org.severstal.data.rmq;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.severstal.data.domain.TenderItem;
import org.severstal.data.repository.DataRepository;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

@Slf4j(topic = "rmq-listener")
@Component
public class Listener {
    @Autowired
    @Qualifier("clickhouse-repository")
    private DataRepository dataRepository;

    @RabbitListener(queues = "data-queue")
    public void worker(String message) throws Exception {
        log.info("Received new RMQ message");
        Gson gson = new Gson();
        List<TenderItem> items = Arrays.stream(gson.fromJson(message, TenderItem[].class)).toList();
        log.info("{} items received", items.size());
        int count = dataRepository.AddParsedItems(items);
        log.info("{} successfully added", count);
    }
}
