public class PageRankNode {
    private String url_;
    private Float pageRank_;
    private Long docId_;
    private String[] links_;
    private Boolean isLeaf_;

    private String separator = " <<sep>> ";
    private String urlSeparator = " <<urlSep>> ";


    public PageRankNode() {
        set("", 0.0f, -1L, new String[]{}, false);
    }

    public PageRankNode(String url, Float pageRank, Long docId, String[] links, Boolean isLeaf) {
        String[] linksCopy = new String[links.length];
        System.arraycopy(links, 0, linksCopy, 0, links.length);
        set(url, pageRank, docId, linksCopy, isLeaf);
    }

    // Constructor for leafs
    public PageRankNode(String url, Float pageRank, Boolean isLeaf) {
        set(url, pageRank, -1L, new String[]{}, isLeaf);
    }

    private void set(String url, Float pageRank, Long docId, String[] links, Boolean isLeaf) {
        url_ = url;
        pageRank_ = pageRank;
        docId_ = docId;
        links_ = links;
        isLeaf_ = isLeaf;
    }

    public String getUrl() {
        return url_;
    }

    public String[] getLinks() {
        return links_;
    }

    public Float getPageRank() {
        return pageRank_;
    }

    public boolean isLeaf() {
        return isLeaf_;
    }

    public Integer linksCount() {
        return links_.length;
    }

    public void setPageRank(Float newPageRank) {
        pageRank_ = newPageRank;
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append("url:=").append(url_).append(separator).append("pageRank:=").append(pageRank_.toString()).append(separator)
                .append("docId:=").append(docId_.toString()).append(separator).append("isLeaf:=").append(isLeaf_.toString()).append(separator);

        string.append(separator).append("linksCount:=").append(linksCount()).append(separator).append("links:=");
        for (String link: links_) {
            string.append(link).append(urlSeparator);
        }
        return string.toString();
    }

    public static PageRankNode fromString(String s) {
        PageRankNode node = new PageRankNode();
        String[] parts = s.split(node.separator);

        node.url_ = parts[0].split(":=")[1];
        node.pageRank_ = Float.parseFloat(parts[1].split(":=")[1]);
        node.docId_ = Long.parseLong(parts[2].split(":=")[1]);
        node.isLeaf_ = Boolean.parseBoolean(parts[3].split(":=")[1]);

        int linksCount = Integer.parseInt(parts[5].split(":=")[1]);
        if (linksCount != 0) {
            node.links_ = parts[6].split(":=")[1].split(node.urlSeparator);
        }

        return node;
    }

    public static boolean isPageRankNodeString(String s) {
        return s.startsWith("url:=");
    }
}
