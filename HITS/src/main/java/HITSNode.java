import java.util.ArrayList;

public class HITSNode {
    private String url_;
    private Float authority_;
    private Float hub_;
    private Long docId_;
    private String[] inLinks_;
    private String[] outLinks_;

    private String separator = " <<sep>> ";
    private String urlSeparator = " <<urlSep>> ";


    public HITSNode() {
        set("", 0.0f, 0.0f, -1L, new String[]{}, new String[]{});
    }

    public HITSNode(String url, Float authority, Float hub, Long docId, String[] inLinks, String[] outLinks) {
        String[] inLinksCopy = new String[inLinks.length];
        System.arraycopy(inLinks, 0, inLinksCopy, 0, inLinks.length);
        String[] outLinksCopy = new String[outLinks.length];
        System.arraycopy(outLinks, 0, outLinksCopy, 0, outLinks.length);
        set(url, authority, hub, docId, inLinksCopy, outLinksCopy);
    }

    public HITSNode(String url, Float authority, Float hub) {
        set(url, authority, hub, -1L, new String[]{}, new String[]{});
    }

    private void set(String url, Float authority, Float hub, Long docId, String[] inLinks, String[] outLinks) {
        url_ = url;
        authority_ = authority;
        hub_ = hub;
        docId_ = docId;
        inLinks_ = inLinks;
        outLinks_ = outLinks;
    }

    public String getUrl() {
        return url_;
    }

    public String[] getInLinks() {
        return inLinks_;
    }

    public String[] getOutLinks() {
        return outLinks_;
    }

    public Float getAuthority() {
        return authority_;
    }

    public Float getHub() {
        return hub_;
    }

    public void setInLinks(ArrayList<String> inLinks) {
        String[] inLinksCopy = new String[inLinks.size()];
        int i = 0;
        for (String link: inLinks) {
            inLinksCopy[i] = link;
            i++;
        }
        inLinks_ = inLinksCopy;
    }

    public void setAuthority(Float authority) {
        authority_ = authority;
    }

    public void setHub(Float hub) {
        hub_ = hub;
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append("url:=").append(url_).append(separator).append("authority:=").append(authority_.toString()).append(separator)
                .append("hub:=").append(hub_.toString()).append(separator).append("docId:=").append(docId_).append(separator);

        string.append("inLinksCount:=").append(Integer.toString(inLinks_.length)).append(separator).append("inLinks:=");
        for (String link: inLinks_) {
            string.append(link).append(urlSeparator);
        }

        string.append(separator).append("outLinksCount:=").append(Integer.toString(outLinks_.length)).append(separator).append("outLinks:=");
        for (String link: outLinks_) {
            string.append(link).append(urlSeparator);
        }

        return string.toString();
    }

    public static HITSNode fromString(String s) {
        Boolean stop = HITSNode.isHITSNodeString(s);


        HITSNode node = new HITSNode();
        String[] parts = s.split(node.separator);

        node.url_ = parts[0].split(":=")[1];
        node.authority_ = Float.parseFloat(parts[1].split(":=")[1]);
        node.hub_ = Float.parseFloat(parts[2].split(":=")[1]);
        node.docId_ = Long.parseLong(parts[3].split(":=")[1]);

        int inLinksCount = Integer.parseInt(parts[4].split(":=")[1]);
        if (inLinksCount != 0) {
            node.inLinks_ = parts[5].split(":=")[1].split(node.urlSeparator);
        }

        int outLinksCount = Integer.parseInt(parts[6].split(":=")[1]);
        if (outLinksCount != 0) {
            node.outLinks_ = parts[7].split(":=")[1].split(node.urlSeparator);
        }

        return node;
    }

    public static boolean isHITSNodeString(String s) {
        return s.startsWith("url:=");
    }
}
