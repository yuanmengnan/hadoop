package info.hbase.api;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import info.soft.utils.chars.JavaPattern;
import info.soft.utils.http.ClientDao;
import info.soft.utils.json.JsonNodeUtils;
import info.weibo.api.core.SinaWeiboConstant;
import info.weibo.api.domain.RequestURL;
import info.weibo.api.domain.SinaDomain;

public class WeiboUserApi {

    private  ClientDao clientDao;

    public WeiboUserApi(ClientDao clientDao) {
        this.clientDao = clientDao;
    }
    public WeiboUserApi(){}

    /**
     * 根据用户id获取用户信息
     * 
     * @param uid
     * @param cookie
     * @param source
     * @return
     */
    public SinaDomain usersShow(String uid, String cookie, String source) {

        RequestURL requestURL = new RequestURL.Builder(SinaWeiboConstant.USERS_SHOW, source)
                .setParams("uid", uid).build();
        String data = clientDao.doGet(requestURL.getURL(), cookie, "UTF-8");
        if (data != null && !data.equals("")) {
            SinaDomain result = parseJsonTree(data);
            return result;
        }else{
            return null;
        }
    }

    /**
     * JSON树解析方法
     * 
     * @param jsonStr：json字符串
     * @return 解析好的JSON对象
     */
    public   SinaDomain parseJsonTree(String jsonStr) {

        SinaDomain result = new SinaDomain();
        JsonNode node = JsonNodeUtils.getJsonNode(jsonStr);
        Iterator<String> fieldNames = node.fieldNames();
        String field = "", value = "";
        JsonNode tnode = null;
        while (fieldNames.hasNext()) {
            field = fieldNames.next();
            if (JsonNodeUtils.getJsonNode(node, field).size() != 0) {
                tnode = JsonNodeUtils.getJsonNode(node, field);
                if (tnode.isArray()) {
                    List<Object> arr = new ArrayList<>();
                    for (JsonNode nodet : tnode) {
                        arr.add(parseJsonTree(nodet.toString()));
                    }
                    result.put(field, arr);
                } else {
                    result.put(field, parseJsonTree(tnode.toString()));
                }
                continue;
            }
            value = node.get(field).toString().replaceAll("\"", "");
            if (value.length() > 0 && value.length() < 15 && JavaPattern.isAllNum(value)
                    && !value.contains("-")) {
                if (value.contains(".") | value.contains("+")) {
                    result.addField(field, Double.parseDouble(value));
                } else {
                    result.addField(field, Long.parseLong(value));
                }
            } else {
                result.addField(field, value);
            }
        }

        return result;
    }

}
