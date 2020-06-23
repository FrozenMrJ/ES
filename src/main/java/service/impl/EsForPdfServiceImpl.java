package service.impl;

import com.itextpdf.awt.geom.Rectangle2D;
import com.itextpdf.text.pdf.PdfDictionary;
import com.itextpdf.text.pdf.PdfName;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.*;
import entity.CommonFile;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import service.EsForPdfService;
import utils.ESUtils;
import utils.PropertiesUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class EsForPdfServiceImpl implements EsForPdfService {
    public static final String pdfIndex = PropertiesUtils.getProperty("es.wxsj.index");     // 数据库名
    public static final String pdfType = PropertiesUtils.getProperty("es.wxsj.types");      // 表名


    @Override
    public void createTable() {
        ESUtils.createIndexAndMapping(pdfIndex,pdfType);
    }

    @Override
    public ArrayList<Map<String, Object>> searchAllPdf() {
        return ESUtils.searchAll(pdfIndex,pdfType);
    }

    @Override
    public ArrayList<Map<String, Object>> searchPdf(String keyword, String... fields) {
        return ESUtils.CombinedQuery(pdfIndex,pdfType,"pdf",keyword,fields);
    }

    @Override
    public void deletePDF(long fileId) {
        TransportClient client = ESUtils.getSingleClient();
        BulkByScrollResponse delete = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("id", fileId))
                .source(pdfIndex)
                .refresh(true)
                .get();
        long count = delete.getDeleted();
        System.out.println(count);
    }

    /**
     * 根据文件路径将PDF导入ES中
     */
    @Override
    public boolean inputPdf(CommonFile file) {
        try{
            TransportClient client = ESUtils.getSingleClient();
            String path = file.getPath();
            String url = file.getUrl();
            String name = file.getName();
            String suffix = path.substring(path.lastIndexOf(".")+1);
            long id = file.getId();

            //1.给定文件
            File pdfFile = new File(path);
            //2.定义一个byte数组，长度为文件的长度
            byte[] pdfData = new byte[(int) pdfFile.length()];
            //3.IO流读取文件内容到byte数组
            FileInputStream inputStream = new FileInputStream(pdfFile);
            inputStream.read(pdfData);
            if (inputStream != null)
                inputStream.close();

            List<PdfPageContentPositions> pdfPageContentPositions = getPdfContentPostionsList(pdfData);
            HashMap<String, Object> map = new HashMap<>();
            for (PdfPageContentPositions pdfPageContentPosition : pdfPageContentPositions) {
                float page = pdfPageContentPosition.getPositions().get(0)[0];       // 获取该页号
                map.put("id",id);
                map.put("title",name);
                map.put("suffix",suffix);
                map.put("page",(int)page);
                map.put("content",pdfPageContentPosition.getContent());
                map.put("url",url);
                map.put("path",path);
//                System.out.println(map);
                String _id = path + "_" + id + "_" + page;   // 唯一ID：文件路径_文件ID
                client.prepareIndex(pdfIndex,pdfType,_id)
                        .setSource(map)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)     // 设置实时刷新
                        .execute()
                        .actionGet();
            }
        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }



    /**
     * 解析pdf
     * @param pdfData
     * @return
     * @throws IOException
     */
    private List<PdfPageContentPositions> getPdfContentPostionsList(byte[] pdfData) throws IOException {
        PdfReader reader = new PdfReader(pdfData);
        List<PdfPageContentPositions> result = new ArrayList<>();
        int pages = reader.getNumberOfPages();
        for (int pageNum = 1; pageNum <= pages; pageNum++) {
            float width = reader.getPageSize(pageNum).getWidth();
            float height = reader.getPageSize(pageNum).getHeight();
            PdfRenderListener pdfRenderListener = new PdfRenderListener(pageNum, width, height);
            //解析pdf，定位位置
            PdfContentStreamProcessor processor = new PdfContentStreamProcessor(pdfRenderListener);
            PdfDictionary pageDic = reader.getPageN(pageNum);
            PdfDictionary resourcesDic = pageDic.getAsDict(PdfName.RESOURCES);
            try {
                processor.processContent(ContentByteUtils.getContentBytesForPage(reader, pageNum), resourcesDic);
            } catch (IOException e) {
                reader.close();
                throw e;
            }
            String content = pdfRenderListener.getContent();
            List<CharPosition> charPositions = pdfRenderListener.getcharPositions();
            List<float[]> positionsList = new ArrayList<>();
            for (CharPosition charPosition : charPositions) {
                float[] positions = new float[]{charPosition.getPageNum(), charPosition.getX(), charPosition.getY()};
                positionsList.add(positions);
            }
            PdfPageContentPositions pdfPageContentPositions = new PdfPageContentPositions();
            pdfPageContentPositions.setContent(content);
            pdfPageContentPositions.setPostions(positionsList);
            result.add(pdfPageContentPositions);
        }
        reader.close();
        return result;
    }


    /**
     * 内部类声明部分 start
     */
    private static class PdfPageContentPositions {
        private String content;
        private List<float[]> positions;
        public String getContent() {
            return content;
        }
        public void setContent(String content) {
            this.content = content;
        }
        public List<float[]> getPositions() {
            return positions;
        }
        public void setPostions(List<float[]> positions) {
            this.positions = positions;
        }
    }

    private static class PdfRenderListener implements RenderListener {
        private int pageNum;
        private float pageWidth;
        private float pageHeight;
        private StringBuilder contentBuilder = new StringBuilder();
        private List<CharPosition> charPositions = new ArrayList<>();
        public PdfRenderListener(int pageNum, float pageWidth, float pageHeight) {
            this.pageNum = pageNum;
            this.pageWidth = pageWidth;
            this.pageHeight = pageHeight;
        }
        public void beginTextBlock() {
        }
        public void renderText(TextRenderInfo renderInfo) {
            List<TextRenderInfo> characterRenderInfos = renderInfo.getCharacterRenderInfos();
            for (TextRenderInfo textRenderInfo : characterRenderInfos) {
                String word = textRenderInfo.getText();
                if (word.length() > 1) {
                    word = word.substring(word.length() - 1, word.length());
                }
                Rectangle2D.Float rectangle = textRenderInfo.getAscentLine().getBoundingRectange();
                float x = (float)rectangle.getX();
                float y = (float)rectangle.getY();

                //这两个是关键字在所在页面的XY轴的百分比
                float xPercent = Math.round(x / pageWidth * 10000) / 10000f;
                float yPercent = Math.round((1 - y / pageHeight) * 10000) / 10000f;
//    CharPosition charPosition = new CharPosition(pageNum, xPercent, yPercent);
                CharPosition charPosition = new CharPosition(pageNum, (float)x, (float)y);
                charPositions.add(charPosition);
                contentBuilder.append(word);
            }
        }
        public void endTextBlock() {
        }
        public void renderImage(ImageRenderInfo renderInfo) {
        }
        public String getContent() {
            return contentBuilder.toString();
        }
        public List<CharPosition> getcharPositions() {
            return charPositions;
        }
    }

    private static class CharPosition {
        private int pageNum = 0;
        private float x = 0;
        private float y = 0;

        public CharPosition(int pageNum, float x, float y) {
            this.pageNum = pageNum;
            this.x = x;
            this.y = y;
        }

        public int getPageNum() {
            return pageNum;
        }

        public float getX() {
            return x;
        }

        public float getY() {
            return y;
        }

        @Override
        public String toString() {
            return "[pageNum=" + this.pageNum + ",x=" + this.x + ",y=" + this.y + "]";
        }
    }
    /**
     * 内部类声明部分 end
     */

    /**
     * 自测用主函数
     */
    public static void main(String[] args) {
        EsForPdfServiceImpl pdfService = new EsForPdfServiceImpl();
        String keyword = "哈佛";      // 搜索关键字

        // 1、建表
        pdfService.createTable();

        // 2、导入PDF数据
//        PDFFile pdf1 = new PDFFile(123456L,"我的于勒叔叔（重复）","D://test2.pdf","localhost");
//        PDFFile[] pdfArr = {pdf1};
//        pdfService.inputPdf(pdfArr);

        // 3、查询
        ArrayList<Map<String, Object>> datas = pdfService.searchAllPdf();
        for(Map map : datas){
            System.out.println("标题："+map.get("title"));
            System.out.println("页号："+map.get("page"));
            System.out.println("内容："+map.get("content"));
        }
//        pdfService.deletePDF(123124L);
    }
}
