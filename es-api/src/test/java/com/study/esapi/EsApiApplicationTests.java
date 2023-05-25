package com.study.esapi;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;


    //测试索引的创建 Request
    @Test
    void testCreateIndex() throws IOException {
        //1、创建
        CreateIndexRequest request = new CreateIndexRequest("study_index");

        //2、执行请求 IndicesClient,请求后获得响应
        CreateIndexResponse createIndexResponse =
                client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    //测试获取索引，判断其是否存在
    @Test
    void testExitsIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("study_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("study_index");
        //删除
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


}
