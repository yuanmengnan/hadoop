package info.weibo.api.example;


import info.soft.utils.http.HttpClientDaoImpl;
import info.soft.utils.json.JsonUtils;
import info.weibo.api.core.SinaWeiboAPI;
import info.weibo.api.domain.SinaDomain;

public class FriendshipsFollowersActive {

	public static void main(String[] args) {

		SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());
		SinaDomain sinaDomain = api.friendshipsFollowersActive("1826792401", 200, 0, "150437216");
		System.out.println(JsonUtils.toJson(sinaDomain));

	}

}
