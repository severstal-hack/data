package org.severstal.data.service;

import com.example.grpc.DataServiceGrpc;
import com.example.grpc.DataServiceOuterClass;
import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.severstal.data.domain.Tender;
import org.severstal.data.repository.DataRepository;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;

@Slf4j(topic = "grpc.service.data")
@GrpcService
public class DataService extends DataServiceGrpc.DataServiceImplBase {
    @Autowired
    private RabbitTemplate template;

    @Autowired
    @Qualifier("clickhouse-repository")
    private DataRepository dataRepository;

    @Override
    public void match(DataServiceOuterClass.MatchRequest request, StreamObserver<DataServiceOuterClass.MatchResponse> responseObserver) {
        try {
            var response = DataServiceOuterClass.MatchResponse.newBuilder();
            var items = dataRepository.Match(request.getPhrase());
            var rpcProducts = items.stream().map(item -> {
                return DataServiceOuterClass.TenderItem.newBuilder()
                        .setLink(item.getLink())
                        .setDomain("")
                        .setName(item.getName())
                        .setCount(item.getCount())
                        .setUnit(item.getUnit()).build();
            }).toList();
            response.addAllItems(rpcProducts);
            responseObserver.onNext(response.build());
        } catch (Exception error) {
            log.error(error.getMessage());
            responseObserver.onNext(DataServiceOuterClass.MatchResponse.newBuilder().build());
            return;
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getProducts(DataServiceOuterClass.EmptyRequest request, StreamObserver<DataServiceOuterClass.GetProductsResponse> responseObserver) {
        try {
            var response = DataServiceOuterClass.GetProductsResponse.newBuilder();
            var products = dataRepository.GetProducts();
            var rpcProducts = products.stream().map(product -> {
                return DataServiceOuterClass.Product.newBuilder().setName(product.getName()).build();
            }).toList();
            response.addAllProducts(rpcProducts);
            responseObserver.onNext(response.build());
        } catch (Exception error) {
            log.error(error.getMessage());
            responseObserver.onNext(DataServiceOuterClass.GetProductsResponse.newBuilder().build());
            return;
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<DataServiceOuterClass.AddRequest> addLinks(StreamObserver<DataServiceOuterClass.AddResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(DataServiceOuterClass.AddRequest req) {
                var tenders = req.getTendersList().stream().map(rpcTender -> new Tender(rpcTender.getLink(), rpcTender.getDomain())).toArray();
                log.info("trying to parse tenders");
                Gson gson = new Gson();
                var json = gson.toJson(tenders);
                log.info("JSON {}", json);

                log.info("send new message to queue");
                template.setExchange("p-exchange");
                template.convertAndSend(json);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("successfully added");
                responseObserver.onNext(DataServiceOuterClass.AddResponse.newBuilder().setCount(0).build());
                responseObserver.onCompleted();
            }
        };
    }
}
