import entity.CommonFile;
import service.impl.EsForFileServiceImpl;

import java.util.ArrayList;
import java.util.Map;

public class esRunner {
    public static void main(String[] args){
        EsForFileServiceImpl fileService = new EsForFileServiceImpl();
        String keyword = "人使用的";      // 搜索关键字      民机运营数据采集研究报告

        // 1、建表
//        fileService.createTable();
//
//        // 2、导入Word数据
//        CommonFile f1 = new CommonFile(10001L,"技术协议-南航","D://test/技术协议-南航.docx","localhost");
//        CommonFile f2 = new CommonFile(10002L,"我的于勒叔叔","D://test/我的于勒叔叔.pdf","localhost");
//        CommonFile f3 = new CommonFile(10003L,"案例","D://test/案例.xps","localhost");
//        CommonFile f4 = new CommonFile(10004L,"微信","D://test/微信.doc","localhost");
//        CommonFile f5 = new CommonFile(10005L,"微信","D://test/微信.docx","localhost");
        CommonFile f10 = new CommonFile(10010L,"微信1","D://test/微信 - 副本 (1).pdf","localhost");
        CommonFile f11 = new CommonFile(10011L,"微信2","D://test/微信 - 副本 (2).pdf","localhost");
        CommonFile f12 = new CommonFile(10012L,"微信3","D://test/微信 - 副本 (3).pdf","localhost");
        CommonFile f13 = new CommonFile(10013L,"微信4","D://test/微信 - 副本 (4).pdf","localhost");
        CommonFile f14 = new CommonFile(10014L,"微信5","D://test/微信 - 副本 (5).pdf","localhost");
        CommonFile f15 = new CommonFile(10015L,"微信6","D://test/微信 - 副本 (6).pdf","localhost");
        CommonFile f16 = new CommonFile(10016L,"微信7","D://test/微信 - 副本 (7).pdf","localhost");
        CommonFile f17 = new CommonFile(10017L,"微信8","D://test/微信 - 副本 (8).pdf","localhost");
        CommonFile f18 = new CommonFile(10018L,"微信9","D://test/微信 - 副本 (9).pdf","localhost");
        CommonFile f19 = new CommonFile(10019L,"微信10","D://test/微信 - 副本 (10).pdf","localhost");
        CommonFile[] fileArr = {f10,f11,f12,f13,f14,f15,f16,f17,f18,f19};
//        fileService.inputFiles(fileArr);
//
//        // 3、查询
//        ArrayList<Map<String, Object>> datas = ESUtils.query3("wxsj_index_v1","wxsj_type","pdf",keyword,"title","content");
//        ArrayList<Map<String, Object>> datas = ESUtils.CombinedQuery("wxsj_index_v1","wxsj_type","",keyword,"title","content");
//        ArrayList<Map<String, Object>> datas = fileService.searchFile(keyword,"pdf","title","content");
        ArrayList<Map<String, Object>> datas = fileService.searchAllFiles(0,100);
        for(Map map : datas){
            System.out.println("标题："+ map.get("title") + "\n");
//            System.out.println("页号："+ map.get("page") + "\n");
//            System.out.println("缩略内容："+ map.get("fragments") + "\n");
//            System.out.println("全部内容："+ map.get("content") + "\n");
        }
//        fileService.deleteFile(10001L,"docx");

    }
}
