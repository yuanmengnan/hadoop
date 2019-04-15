package info.hbase.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import info.soft.utils.http.HttpClientDaoImpl;

/**
 * 线程安全的HTTP短连接请求处理程序
 * 
 * @author gy
 * 
 */
public class HttpHandle extends HttpClientDaoImpl {

    //public static void main(String[] args) {
        // String s=;
        // new HttpHandle().doPost("http://localhost:2900/data/toHbase", "keinnhkj",s);
    //}

    @Override
    public String doPost(String url, String data) {
        return doPost(url,null,data);
    }

    @Override
    public String doPost(String url, String rowKey, String values) {

        CloseableHttpClient httpClient = getHttpClient();
        try {
            HttpPost post = new HttpPost(url); //
            // 创建参数列表
            List<NameValuePair> list = new ArrayList<NameValuePair>();
            if (rowKey != null)
                list.add(new BasicNameValuePair("rowKey", rowKey));
            list.add(new BasicNameValuePair("values", values));
            // url格式编码
            UrlEncodedFormEntity uefEntity = new UrlEncodedFormEntity(list, "UTF-8");
            post.setEntity(uefEntity);
            // 执行请求
            CloseableHttpResponse httpResponse = httpClient.execute(post);
            try {
                HttpEntity entity = httpResponse.getEntity();
                return EntityUtils.toString(uefEntity);
            } finally {
                httpResponse.close();
            }

        } catch (UnsupportedEncodingException e) {
            return e.getMessage();
        } catch (IOException e) {
            return e.getMessage();
        } finally {
            try {
                closeHttpClient(httpClient);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private CloseableHttpClient getHttpClient() {

        return HttpClients.createDefault();
    }

    private void closeHttpClient(CloseableHttpClient client) throws IOException {

        if (client != null) {
            client.close();
        }
    }

    @Override
    public String doGet(String url, HashMap<String, String> headers, String cookie,
            String charset) {

        // 创建客户端连接
        HttpClient client = new HttpClient();
        // 设置代理
        // IPProxy proxy = IPProxyPool.getProxy();
        // client.getHostConfiguration().setProxy(proxy.getIp(), proxy.getPort());
        // 设置连接管理器取出连接超时时间
        client.getParams().setConnectionManagerTimeout(1 * 1000);
        // 设置建立连接超时时间
        client.getHttpConnectionManager().getParams().setConnectionTimeout(2 * 1000);
        // 设置响应超时时间
        client.getHttpConnectionManager().getParams().setSoTimeout(4 * 1000);
        // 禁用Nagle算法，降低网络延迟并提高性能
        client.getHttpConnectionManager().getParams().setTcpNoDelay(true);
        // 创建请求方法
        HttpMethod method = new GetMethod(url);
        // 禁用Cookie提高性能
        method.getParams().setCookiePolicy(CookiePolicy.DEFAULT);
        method.setRequestHeader("Cookie", cookie);
        // 采取服务端关闭连接
        method.setRequestHeader("Connection", "close");
        // HttpClient
        // 默认请求失败属于网络层面的请求失败会重试三次，但是如果请求成功并返回200响应码后，服务端关闭Socket连接造成响应超时或中断，则直接抛出异常，不会触发自带的重试操作
        // 下面的重试保护属于业务层面的重试，最大限度的保证每次请求都成功
        // 重试计数器
        int retry = 0;
        // 异常
        RuntimeException exception = null;
        // 开启重试保护
        do {
            try {
                // 执行请求
                client.executeMethod(method);
                // 构造响应数据
                StringBuffer sb = new StringBuffer();
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(method.getResponseBodyAsStream()));) {
                    String str = "";
                    while ((str = br.readLine()) != null) {
                        sb.append(str);
                    }
                }
                // 返回
                return sb.toString();
            } catch (URIException e) {
                exception = new RuntimeException(e);
            } catch (IOException e) {
                exception = new RuntimeException(e);
            } catch (Exception e) {
                exception = new RuntimeException(e);
            } finally {
                // 关闭响应
                method.releaseConnection();
            }
        } while (++retry < 3);// 最大重试3次
        // 三次重试失败，抛出异常
        throw exception;
    }
}
