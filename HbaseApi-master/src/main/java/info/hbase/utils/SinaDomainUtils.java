
package info.hbase.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.hbase.domain.Geo;
import info.hbase.domain.User;
import info.hbase.domain.Visible;
import info.weibo.api.domain.SinaDomain;


/**
 * 新浪对象转换工具
 *
 * @author gy
 *
 */
public class SinaDomainUtils {

    private static final Logger logger = LoggerFactory.getLogger(SinaDomainUtils.class);

    public static User getUser(SinaDomain sinaDomain) {

        Date date = new Date();
        User user =
                new User.Builder(getLong(sinaDomain, "id"), getString(sinaDomain, "screen_name"),
                        getString(sinaDomain, "name"),
                        tranSinaApiDate(getString(sinaDomain, "created_at")))
                                .setProvince(getInt(sinaDomain, "province"))
                                .setCity(getInt(sinaDomain, "city"))
                                .setLocation(getString(sinaDomain, "location"))
                                .setDescription(getString(sinaDomain, "description"))
                                .setUrl(getString(sinaDomain, "url"))
                                .setProfile_image_url(getString(sinaDomain, "profile_image_url"))
                                .setDomain(getString(sinaDomain, "domain"))
                                .setGender(getString(sinaDomain, "gender"))
                                .setFollowers_count(getInt(sinaDomain, "followers_count"))
                                .setFriends_count(getInt(sinaDomain, "friends_count"))
                                .setWeihao(getString(sinaDomain, "weihao"))
                                .setStatuses_count(getInt(sinaDomain, "statuses_count"))
                                .setFavourites_count(getInt(sinaDomain, "favourites_count"))
                                .setVerified_type(Integer.parseInt(
                                        sinaDomain.getFieldValue("verified_type").toString()))
                                .setRemark(getString(sinaDomain, "remark"))
                                .setAvatar_large(getString(sinaDomain, "avatar_large"))
                                .setVerified_reason(getString(sinaDomain, "verified_reason"))
                                .setBi_followers_count(getInt(sinaDomain, "bi_followers_count"))
                                .build();
        user.setLasttime(date);
        return user;
    }

    public static Date tranSinaApiDate(String timeStr) {

        try {
            if (timeStr != null)
                return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US).parse(timeStr);
             return new Date();
        } catch (ParseException e) {
            logger.info("The current time is wrong");
            return new Date();
        }
    }

    private static boolean getBoolean(SinaDomain sinaDomain, String key) {

        try {
            return sinaDomain.getFieldValue(key).toString().equalsIgnoreCase("true") ? Boolean.TRUE
                    : Boolean.FALSE;
        } catch (Exception e) {
            return Boolean.FALSE;
        }
    }

    private static int getInt(SinaDomain sinaDomain, String key) {

        try {
            return Integer.parseInt(sinaDomain.getFieldValue(key).toString());
        } catch (Exception e) {
            return 0;
        }
    }

    private static long getLong(SinaDomain sinaDomain, String key) {

        try {
            return (long) sinaDomain.getFieldValue(key);
        } catch (Exception e) {
            return 0L;
        }
    }

    private static String getString(SinaDomain sinaDomain, String key) {

        try {
            return sinaDomain.getFieldValue(key).toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static SinaDomain getSinaDomain(SinaDomain sinaDomain, String key) {

        try {
            return (SinaDomain) sinaDomain.get(key);
        } catch (Exception e) {
            return new SinaDomain();
        }
    }

    private static Geo getGeo(SinaDomain sinaDomain, String key) {

        try {
            return (Geo) sinaDomain.get(key);
        } catch (Exception e) {
            return new Geo();

        }
    }

    private static Visible getVisible(SinaDomain sinaDomain, String key) {

        try {
            return (Visible) sinaDomain.get(key);
        } catch (Exception e) {
            return new Visible();
        }
    }

    //public static void main(String[] args) {

        //System.out.println(tranSinaApiDate(null));
        // System.out.println((Geo)null);
        // SinaDomain sinaDomain = new SinaDomain();
        // System.out.println(getGeo(sinaDomain,"geo").getCoordinates());
    //}
}
