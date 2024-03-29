package org.severstal.data.service;

import com.example.grpc.DataServiceGrpc;
import com.example.grpc.DataServiceOuterClass;
import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.severstal.data.domain.Tender;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
@GrpcService
public class DataService extends DataServiceGrpc.DataServiceImplBase {
    @Autowired
    private RabbitTemplate template;


    @Override
    public StreamObserver<DataServiceOuterClass.AddRequest> addLinks(StreamObserver<DataServiceOuterClass.AddResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(DataServiceOuterClass.AddRequest req) {
                List<Tender> tenders = req.getTendersList().stream().map(rpcTender -> new Tender(rpcTender.getLink(), rpcTender.getDomain())).toList();
                template.setExchange("p-exchange");
                Gson gson = new Gson();
                log.debug("Trying to send to Queue {} tenders", tenders.size());
                template.convertAndSend(gson.toJson(tenders));
                log.info("successfully added");
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
