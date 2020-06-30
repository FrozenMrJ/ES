package utils;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

public class ESUtils {

    public static final String CLUSTER_NAME = PropertiesUtils.getProperty("es.cluster.name");
    public static final String HOST_IP = PropertiesUtils.getProperty("es.host.ip");
    public static final int TCP_PORT = Integer.parseInt(PropertiesUtils.getProperty("es.tcp.port"));
    private static volatile TransportClient client;

    static Settings settings = Settings.builder()
            .put("cluster.name",CLUSTER_NAME)
            .build();

    // 创建单例模式的TransportClient对象
    public static TransportClient getSingleClient(){
        if(client == null){
            synchronized (ESUtils.class){
                if(client == null){
                    try{
                        client = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(
                                        InetAddress.getByName(HOST_IP),TCP_PORT));
                    }catch (UnknownHostException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        return client;
    }

    // 封装一个创建索引的方法，索引名、分片数和副本数作为参数传入
    public static IndicesAdminClient getAdminClient(){
        return getSingleClient().admin().indices();
    }

    /**
     * 创建索引
     * @param indexName 索引名
     * @param shards    冗余度
     * @param replicas  分片数
     * @return
     */
    public static boolean createIndex(String indexName,int shards,int replicas){
        Settings settings = Settings.builder()
                .put("index.number_of_shards",shards)
                .put("index.number_of_replicas",replicas)
                .build();

        IndicesAdminClient adminClient = getAdminClient();
        IndicesExistsResponse response = adminClient.exists(new IndicesExistsRequest(indexName.toLowerCase())).actionGet();

        if(response.isExists()){
            System.out.println("索引" + indexName + "已存在");

//            adminClient.prepareDelete(indexName.toLowerCase()).get();
            return false;
        }else{
            CreateIndexResponse createIndexResponse = adminClient
                    .prepareCreate(indexName.toLowerCase())
                    .setSettings(settings)
                    .execute().actionGet();
            boolean isIndexCreated = createIndexResponse.isAcknowledged();
            if(isIndexCreated){
                System.out.println("索引" + indexName + "创建成功");
            }else {
                System.out.println("索引" + indexName + "创建失败");
            }
            return isIndexCreated;
        }
    }

    public static ArrayList<Map<String, Object>> query3(String index,String types,String suffix,String keyword,String... fields){
        client = ESUtils.getSingleClient();
        BoolQueryBuilder builder = QueryBuilders.boolQuery();       // 最上级为全查

        // 第一筛选条件：关键字查询
        MultiMatchQueryBuilder builder1 = QueryBuilders.multiMatchQuery(keyword, fields).operator(Operator.AND);
        BoolQueryBuilder queryBuilder = builder.must(builder1);     // 拼接查询json，之间关系为must(and)

        // 输入了后缀条件
        if(suffix != null && suffix.length() > 0){
            // 第二筛选条件为：后缀
            MatchQueryBuilder builder2 = QueryBuilders.matchQuery("suffix", suffix).operator(Operator.AND);
            queryBuilder = builder.must(builder2);
        }
//        System.out.println("查询json: " + queryBuilder);

        //设置高亮数据
        HighlightBuilder hiBuilder=new HighlightBuilder();
        hiBuilder.preTags("<font style='color:red'>");
        hiBuilder.postTags("</font>");
        hiBuilder.field("content");

        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(types)
                .setQuery(queryBuilder)
                .highlighter(hiBuilder)
                .execute().actionGet();

        SearchHits hits = searchResponse.getHits();
        System.out.println("共搜到:"+hits.getTotalHits()+"条结果!");
        ArrayList<Map<String, Object>> pdfList = new ArrayList<>();
        for (SearchHit hit :hits) {
            System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());
            System.out.println("遍历高亮集合，打印高亮片段:");
            Text[] text = hit.getHighlightFields().get("content").getFragments();
            for (Text str : text) {
                System.out.println(str);
            }
//            pdfList.add(pdfMap);
        }
        return pdfList;
    }


    /**
     * 查找全部的文档
     * @param index 索引名（数据库名）
     * @param type  类型名（表名）
     * @param from 从第几个开始，默认0
     * @param size 显示数据的行数，默认10
     */
    public static ArrayList<Map<String, Object>> searchAll(String index,String type,int from,int size) {
        client = ESUtils.getSingleClient();
        // Query
        QueryBuilder matchAll = QueryBuilders.matchAllQuery();
        // Search
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        searchRequestBuilder.setTypes(type);
        searchRequestBuilder.setQuery(matchAll);
        searchRequestBuilder.setFrom(from >= 0 ? from : 0);
        searchRequestBuilder.setSize(size >= 0 ? size : 10);

        // 执行
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        ArrayList<Map<String, Object>> pdfList = new ArrayList<>();
        for (SearchHit hit :hits) {
            Map<String, Object> pdfMap = hit.getSourceAsMap();
            // 剃重
            String id = pdfMap.get("id").toString();
            if(pdfList.size() > 0){
                boolean exist = false;
                for(Map<String, Object> map : pdfList){
                    if(Objects.equals(map.get("id").toString(), id)) {
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    pdfList.add(pdfMap);
                }
            }else{
                pdfList.add(pdfMap);
            }
        }
        return pdfList;
    }

    /**
     * 组合查询
     * @param index 索引名
     * @param types 类型名
     * @param suffix 后缀 查找的文件后缀名，不填为全查
     * @param keyword  搜索的关键字
     * @param fields   搜索的域
     * @param from 从第几个文档开始查询，默认为0
     * @param size 查询多少个文档，默认为10
     * @return
     */
    @Test
    public static ArrayList<Map<String, Object>> CombinedQuery(String index, String types, String suffix,
                                                               String keyword, int from,int size,String... fields){
        client = ESUtils.getSingleClient();
        // 最上级为全查
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        // 第一筛选条件：关键字查询
        MultiMatchQueryBuilder builder1 = QueryBuilders.multiMatchQuery(keyword, fields).operator(Operator.AND);
        // 拼接查询json，之间关系为must(and)
        BoolQueryBuilder queryBuilder = builder.must(builder1);

        if(suffix != null && suffix.length() > 0){
            // 第二筛选条件为：后缀
            MatchQueryBuilder builder2 = QueryBuilders.matchQuery("suffix", suffix).operator(Operator.AND);
            queryBuilder = builder.must(builder2);
        }
        System.out.println("查询json: \n" + queryBuilder);

        //设置高亮数据
        HighlightBuilder hiBuilder=new HighlightBuilder();
        hiBuilder.preTags("<font style='color:red'>");
        hiBuilder.postTags("</font>");
        hiBuilder.field("content");

        // 执行查询
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(types)
                .highlighter(hiBuilder)
                .setQuery(queryBuilder)
                .setFrom(from >= 0 ? from : 0)
                .setSize(size >= 0 ? size : 10)
                .execute().actionGet();

        SearchHits hits = searchResponse.getHits();
        System.out.println("共搜到:"+hits.getTotalHits()+"条结果!");
        ArrayList<Map<String, Object>> pdfList = new ArrayList<>();
        for (SearchHit hit :hits) {
            Map<String, Object> pdfMap = hit.getSourceAsMap();
            // 剃重
            String id = pdfMap.get("id").toString();
            if(pdfList.size() > 0){
                boolean exist = false;
                for(Map<String, Object> map : pdfList){
                    if(Objects.equals(map.get("id").toString(), id)) {
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    Text[] text = hit.getHighlightFields().get("content").getFragments();   // 获取高亮字段
                    pdfMap.put("fragments",text[0]);        // 将高亮字段的第一段放入
                    pdfList.add(pdfMap);
                }
            }else{
                Text[] text = hit.getHighlightFields().get("content").getFragments();   // 获取高亮字段
                pdfMap.put("fragments",text[0]);        // 将高亮字段的第一段放入
                pdfList.add(pdfMap);
            }
        }
        return pdfList;
    }

    /**
     * 创建索引 和 Mapping
     */
    public static void createIndexAndMapping(String index,String type){
        // 1、创建索引
        boolean flag = ESUtils.createIndex(index, 3, 0);    // 是否新建

        if(flag){
            // 2、设置Mapping
            try{
                XContentBuilder builder = XContentFactory.jsonBuilder()
                        .startObject()
                            .startObject("properties")
                                .startObject("id")
                                    .field("type","long")
//                                    .field("fielddata","true")
                                .endObject()
                                .startObject("title")
                                    .field("type","text")
                                    .field("analyzer","ik_max_word")
                                    .field("search_analyzer","ik_max_word")
                                .endObject()
                                .startObject("path")
                                    .field("type","keyword")
                                .endObject()
                                .startObject("suffix")
                                    .field("type","text")
                                .endObject()
                                .startObject("url")
                                    .field("type","keyword")
                                .endObject()
                                .startObject("page")
                                .field("type","long")
                                .endObject()
                                .startObject("content")
                                    .field("type","text")
                                    .field("store","true")
                                    .field("analyzer","ik_max_word")
                                    .field("search_analyzer","ik_max_word")
                                .endObject()
                            .endObject()
                        .endObject();
                PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type).source(builder);
                client.admin().indices().putMapping(mappingRequest).actionGet();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public static void deleteFiles(String index,String types,long fileId,String suffix){
        TransportClient client = ESUtils.getSingleClient();
        BoolQueryBuilder builder = QueryBuilders.boolQuery();       // 最上级为全查

        // 第一筛选条件：关键字查询
        MultiMatchQueryBuilder builder1 = QueryBuilders.multiMatchQuery(fileId, "id").operator(Operator.AND);
        BoolQueryBuilder queryBuilder = builder.must(builder1);     // 拼接查询json，之间关系为must(and)

        if(suffix != null && suffix.length() > 0){
            // 第二筛选条件为：后缀
            MatchQueryBuilder builder2 = QueryBuilders.matchQuery("suffix", suffix).operator(Operator.AND);
            queryBuilder = builder.must(builder2);
        }

        BulkByScrollResponse delete = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(queryBuilder)
                .source(index)
                .refresh(true)
                .get();
        System.out.println("删除了" + delete.getDeleted() + "条数据");
    }
}
