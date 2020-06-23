package service;

import entity.CommonFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public interface EsForPdfService {
    /**
     * 根据文件路径将PDF导入ES中
     * @throws IOException
     */
    boolean inputPdf(CommonFile file) throws IOException;

    /**
     * 根据配置文件建表
     */
    void createTable();

    ArrayList<Map<String, Object>> searchAllPdf();

    /**
     * 根据关键字查询ES中指定范围的内容
     * @param keyword   关键字
     * @param fields    范围：title 标题     content 内容
     * @return
     */
    ArrayList<Map<String, Object>> searchPdf(String keyword, String... fields);

    /**
     * 根据文件ID删除ES中对应数据
     * @param fileId    文件ID
     */
    void deletePDF(long fileId);

}
