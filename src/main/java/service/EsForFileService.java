package service;

import entity.CommonFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public interface EsForFileService {
    /**
     * 将文件导入ES中
     * @param files 文件数组
     * @throws IOException
     */
    boolean inputFiles(CommonFile[] files) throws IOException;

    /**
     * 根据配置文件es.properties建表
     */
    void createTable();

    /**
     * 查询当前ES中所有的文档
     * @return  一个Map就是ES中的一条数据，可获取的key为：path,id,page,title,suffix,content,url,fragments（带有高亮的关键字缩略图）
     */
    ArrayList<Map<String, Object>> searchAllFiles();

    /**
     * 根据关键字查询ES中指定范围的内容
     * @param keyword   关键字
     * @param fields    范围：title 标题     content 内容
     * @param suffix    文件后缀名 docx、doc、pdf...
     * @return  一个Map就是ES中的一条数据，可获取的key为：path,id,page,title,suffix,content,url,fragments（带有高亮的关键字缩略图）
     */
    ArrayList<Map<String, Object>> searchFile(String keyword,String suffix, String... fields);

    /**
     * 删除ES中对应数据
     * @param fileId 文件ID
     * @param suffix 文件后缀，""为只根据文件ID删除
     */
    void deleteFile(long fileId,String suffix);

}
