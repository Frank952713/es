package com.study.esapi;

import com.alibaba.fastjson.JSON;
import com.study.esapi.com.study.esapi.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

//---------------------------------------------索引的API操作----------------------------------

    //测试索引的创建 Request  PUT study_index
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

//---------------------------------------------文档的API操作----------------------------------

    //测试添加文档
    @Test
    void testAddDocuments() throws IOException {
        User user = new User("张三", 3);
        IndexRequest request = new IndexRequest("study_index");

        //规则  put/study_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueDays(1));
        request.timeout("1s");

        //*********  将数据放进请求 json  (添加是 source())********
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //发送请求，获取响应的结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());//对应我们命令返回的状态 CREATED
    }

    //判断文档是否存在
    @Test
    void testIsDocumentsExits() throws IOException {
        GetRequest getRequest = new GetRequest("study_index", "1");

        //不获取返回的 _source 的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档的信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("study_index", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(getResponse.getSourceAsString());//打印文档的内容（或者可以获取一个map）
        System.out.println(getResponse);//返回的全部内容和命令是一样的
    }

    //更新文档的信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("study_index", "1");
        updateRequest.timeout("1s");

        User user = new User("李四", 16);
        //*********  将数据放进请求 json   (更新是 doc() )********
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    //删除文档记录
    @Test
    void testDeleteRequest() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("study_index", "1");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }


    //批量插入数据！！！！！
    @Test
    void testBulkRequest() throws IOException {

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10");

        ArrayList<User> userList = new ArrayList<>();

        userList.add(new User("zhangsan1", 29));
        userList.add(new User("zhangsan2", 29));
        userList.add(new User("zhangsan3", 29));
        userList.add(new User("zhangsan4", 29));

        //批处理请求
        for (int i = 0; i < userList.size(); i++) {

            //批量更新和批量删除，就在这里改动对于的请求就可以了
            bulkRequest.add(
                    new IndexRequest("study_index")
                            .id("" + (i + 1))
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        boolean b = bulkResponse.hasFailures();//是否失败，返回 false，表示成功 ！
    }

    //******** 查询 (自定义) ***************
    //SearchRequest 搜索请求
    //SearchSourceBuilder 条件构造
    //HighlightBuilder 构建高亮
    // TermQueryBuilder 精确查询
    // MatchAllQueryBuilder 匹配所有查询
    // xxx QueryBuilder 对于所有的命令

    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("study_index");
        //构建搜索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询条件， 使用 QueryBuilders 工具来实现
        //QueryBuilders.termQuery() 精确匹配
        //QueryBuilders.matchAllQuery() 匹配所有

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "zhangsan1");
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        searchSourceBuilder.query(termQueryBuilder);
        //规则
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //将 searchSourceBuilder 放进请求
        searchRequest.source(searchSourceBuilder);

        //获取请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // Hits 、输出
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("==================================================");
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }
    }

}
