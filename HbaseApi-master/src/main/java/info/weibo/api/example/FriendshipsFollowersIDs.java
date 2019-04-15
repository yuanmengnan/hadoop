package info.weibo.api.example;

import info.soft.utils.http.HttpClientDaoImpl;
import info.soft.utils.json.JsonUtils;
import info.weibo.api.core.SinaWeiboAPI;
import info.weibo.api.domain.SinaDomain;

public class FriendshipsFollowersIDs {

	public static void main(String[] args) {

		SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());
		String cookie = "SINAGLOBAL=9301345128864.139.1491446277758; UOR=,,news.ifeng.com; _s_tentry=-; Apache=206939632679.63467.1491967474306; ULV=1491967474344:2:2:1:206939632679.63467.1491967474306:1491446277765; JSESSIONID=FC0DE886ACED57FB721E7F9B1D425169; SUB=_2AkMvsRFSf8NxqwJRmP4TzmPia49-zwnEieKZ7eCJJRMxHRl-yT9kql4PtRC7E4189FEIC3vF_0yz2Lcls9HHWQ..; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WWm1iHcP3K4ugpbyQSvXjfU; login_sid_t=ae76a351396331de03e2b41c057193e4";
		SinaDomain sinaDomain = api.friendshipsFollowersIDs("1826792401", 500, 0, cookie, "150437216");
		System.out.println(JsonUtils.toJson(sinaDomain));

	}

}
