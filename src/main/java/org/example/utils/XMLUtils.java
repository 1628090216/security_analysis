package org.example.utils;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class XMLUtils {
    private Document doc;

    public XMLUtils(String xmlFilePath) throws Exception {
        InputStream input = XMLUtils.class.getClassLoader().getResourceAsStream(xmlFilePath);
        doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(input);
    }

    // 获取所有 <sql> 标签
    public Map<String, String> getAllSqls() {
        return getSqlMapByTag("sql");
    }

//    // 获取所有 <rule> 标签
//    public Map<String, String> getAllRuleSqls() {
//        return getSqlMapByTag("rule");
//    }

    // 根据 id 获取 <sql>
    public String getSqlById(String id) {
        return getSqlByIdAndTag(id, "sql", true);
    }

//    // 根据 id 获取 <rule>
//    public String getRuleSqlById(String id) {
//    return getSqlByIdAndTag(id, "rule", true);
//}

    private Map<String, String> getSqlMapByTag(String tagName) {
        Map<String, String> map = new LinkedHashMap<>();
        NodeList list = doc.getElementsByTagName(tagName);
        for (int i = 0; i < list.getLength(); i++) {
            Element el = (Element) list.item(i);
            map.put(el.getAttribute("id"), el.getTextContent().trim());
        }
        return map;
    }

    private String getSqlByIdAndTag(String id, String tagName, boolean throwIfNotFound) {
    NodeList list = doc.getElementsByTagName(tagName);
    for (int i = 0; i < list.getLength(); i++) {
        Element el = (Element) list.item(i);
        if (id.equals(el.getAttribute("id"))) {
            String sql = el.getTextContent().trim();
            if (sql.isEmpty()) {
                throw new RuntimeException("❌ <" + tagName + " id='" + id + "'> 内容为空！");
            }
            return sql;
        }
    }

    if (throwIfNotFound) {
        throw new RuntimeException(
            "❌ 未找到 <" + tagName + " id='" + id + "'>\n" +
            "  可能原因：\n" +
            "  1. ID 写错了\n" +
            "  2. 你可能用错了方法（比如该用 getRuleSqlById 却用了 getSqlById）\n" +
            "  3. XML 文件中没有这个 ID"
        );
    }
    return null;
}
}
