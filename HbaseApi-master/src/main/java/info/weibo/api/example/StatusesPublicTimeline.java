package info.weibo.api.example;

import info.soft.utils.http.HttpClientDaoImpl;
import info.soft.utils.json.JsonUtils;
import info.weibo.api.core.SinaWeiboAPI;
import info.weibo.api.domain.SinaDomain;

/**
 * 返回最新的200条公共微博，返回结果非完全实时
 * 
 */
public class StatusesPublicTimeline {

	public static void main(String[] args) {

		SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());
		SinaDomain sinaDomain = api.statusesPublicTimeline(2);
		System.out.println(JsonUtils.toJson(sinaDomain));

	}

}
