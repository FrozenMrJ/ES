package entity;


public class PDFFile {
  private long id;
  private String name;

  private String path;

  private String url;

  public PDFFile() {
  }

  public PDFFile(long id, String name, String path, String url) {
    this.id = id;
    this.name = name;
    this.path = path;
    this.url = url;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public String getUrl() {
    return url;
  }
}
