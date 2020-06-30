package service.impl;

import entity.CommonFile;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.ooxml.POIXMLDocument;
import org.apache.poi.ooxml.extractor.POIXMLTextExtractor;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import service.EsForFileService;
import utils.ESUtils;
import utils.PropertiesUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class EsForFileServiceImpl implements EsForFileService {
    private static final String esIndex = PropertiesUtils.getProperty("es.wxsj.index");     // 数据库名
    private static final String esType = PropertiesUtils.getProperty("es.wxsj.types");      // 表名


    @Override
    public void createTable() {
        ESUtils.createIndexAndMapping(esIndex, esType);
    }

    @Override
    public ArrayList<Map<String, Object>> searchAllFiles(int from,int size) {
        return ESUtils.searchAll(esIndex, esType,from,size);
    }

    /**
     * 根据条件查询file信息
     * @param suffix 后缀 查找的文件后缀名，输入""为全查
     * @param keyword  搜索的关键字
     * @param fields   搜索的域
     * @return
     */
    @Override
    public ArrayList<Map<String, Object>> searchFile(String keyword,String suffix, int from,int size,String... fields) {
        return ESUtils.CombinedQuery(esIndex, esType,suffix,keyword,from,size,fields);
    }

    @Override
    public void deleteFile(long fileId,String suffix) {
        ESUtils.deleteFiles(esIndex, esType,fileId,suffix);
    }

    /**
     * 根据文件路径将PDF导入ES中
     * @throws IOException
     */
    @Override
    public boolean inputFiles(CommonFile[] files) {
        try {
            TransportClient client = ESUtils.getSingleClient();
            for (CommonFile file : files) {
                HashMap<String, Object> map = new HashMap<>();
                String filePath = file.getPath();
                String url = file.getUrl();
                String name = file.getName();
                String suffix = filePath.substring(filePath.lastIndexOf(".")+1);
                long id = file.getId();
                String buffer = null;
                boolean flag = true;   // 是否支持类型
                List<String> linList = new ArrayList<>();
                if(filePath.endsWith(".pdf")){      // pdf实现分页,单独处理
                    EsForPdfServiceImpl pdfService = new EsForPdfServiceImpl();
                    boolean b = pdfService.inputPdf(file);
                    if(b)
                        System.out.println("文件:" + filePath + "导入成功");
                    else
                        System.out.println("文件:" + filePath + "导入失败");
                }else if (filePath.endsWith(".doc")) {
                    InputStream is = new FileInputStream(new File(filePath));
                    WordExtractor ex = new WordExtractor(is);
                    buffer = ex.getText();

                    if (buffer.length() > 0) {
                        //使用回车换行符分割字符串
                        String[] arry = buffer.split("\\r\\n");
                        for (String string : arry) {
                            linList.add(string.trim());
                        }
                    }
                } else if (filePath.endsWith(".docx")) {
                    OPCPackage opcPackage = POIXMLDocument.openPackage(filePath);
                    POIXMLTextExtractor extractor = new XWPFWordExtractor(opcPackage);
                    buffer = extractor.getText();
                    extractor.close();

                    if (buffer.length() > 0) {
                        //使用换行符分割字符串
                        String[] arry = buffer.split("\\n");
                        for (String string : arry) {
                            linList.add(string.trim());
                        }
                    }
                }else{
                    flag = false;
                }
                if (flag && !filePath.endsWith(".pdf")){      // 支持该类型并且不是pdf
                    // 存ES
                    map.put("id", id);
                    map.put("title", name);
                    map.put("content", buffer);
                    map.put("url", url);
                    map.put("page", 0);
                    map.put("path", filePath);
                    map.put("suffix", suffix);
                    System.out.println("文件:" + filePath + "导入成功");
                    String _id = filePath + "_" + id;   // 唯一ID：文件路径_文件ID
                    client.prepareIndex(esIndex, esType, _id)
                            .setSource(map)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)     // 设置实时刷新
                            .execute()
                            .actionGet();
                }else{
                    String end = filePath.substring(filePath.lastIndexOf("."));
                    if(!end.equals(".pdf"))
                        System.out.println("暂不支持" + end + "格式");}
            }
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 自测用主函数
     */
    public static void main(String[] args){
        EsForFileServiceImpl wordService = new EsForFileServiceImpl();
        String keyword = "BRP52134124150";      // 搜索关键字

        // 1、建表
//        wordService.createTable();

        // 2、导入Word数据
//        CommonFile word1 = new CommonFile(123456L,"技术协议-南航","D://test/技术协议-南航.docx","localhost");
//        CommonFile[] wordArr = {word1};
//        wordService.inputFiles(wordArr);

        // 3、查询
//        ArrayList<Map<String, Object>> datas = wordService.searchAllWord();
//        ArrayList<Map<String, Object>> datas = wordService.searchWord(keyword,"title","content");
//        for(Map map : datas){
//            System.out.println("标题："+map.get("title"));
//            System.out.println("页号："+map.get("page"));
//            System.out.println("内容："+map.get("content"));
//        }
//        wordService.deleteWord(123456L);
    }
}
