package info.hbase.domain;


import java.io.Serializable;
import java.util.Date;

/**
 * 新浪用户基础信息数据模型
 *
 * @author gy
 *
 */
public class User implements Serializable {

    private static final long serialVersionUID = -4574543153431266465L;

    // 用户id
    private long id;
    
    // 昵称
    private String screen_name;
    // 名称
    private String name;
    // 省编码
    private int province;
    // 市编码
    private int city;
    // 位置信息
    private String location;
    // 描述信息
    private String description;
    // 主页地址
    private String url;
    // 头像地址
    private String profile_image_url;
    // 个性化域名
    private String domain;
    // 微信号
    private String weihao;
    // 性别
    private String gender;
    // 粉丝数
    private int followers_count;
    // 关注数
    private int friends_count;
    // 微博数
    private int statuses_count;
    // 收藏数
    private int favourites_count;
    // 用户创建时间
    private Date created_at;
    // 是否认证
    private boolean verified;
    // 认证类型
    private int verified_type;
    // 备注
    private String remark;
    // 大头像地址
    private String avatar_large;
    // 认证信息
    private String verified_reason;
    // 双向关注数
    private int bi_followers_count;
    
    private Date lasttime;
    private Weibo weibo;
    public User() {
        super();
    }

    public User(Builder builder) {
        this.id = builder.id;
        this.screen_name = builder.screen_name;
        this.name = builder.name;
        this.province = builder.province;
        this.city = builder.city;
        this.location = builder.location;
        this.description = builder.description;
        this.url = builder.url;
        this.profile_image_url = builder.profile_image_url;
        this.domain = builder.domain;
        this.weihao = builder.weihao;
        this.gender = builder.gender;
        this.followers_count = builder.followers_count;
        this.friends_count = builder.friends_count;
        this.statuses_count = builder.statuses_count;
        this.favourites_count = builder.favourites_count;
        this.created_at = builder.created_at;
        this.verified = builder.verified;
        this.verified_type = builder.verified_type;
        this.remark = builder.remark;
        this.avatar_large = builder.avatar_large;
        this.verified_reason = builder.verified_reason;
        this.bi_followers_count = builder.bi_followers_count;
        this.weibo=builder.weibo;
    }

    public static class Builder {

        private long id;
        private String idstr;
        private int uclass;
        private String screen_name = "";
        private String name = "";
        private int province;
        private int city;
        private String location = "";
        private String description = "";
        private String url = "";
        private String profile_image_url = "";
        private String profile_url = "";
        private String domain = "";
        private String weihao = "";
        private String gender = "";
        private int followers_count;
        private int friends_count;
        private int pagefriends_count;
        private int statuses_count;
        private int favourites_count;
        private Date created_at;
        private boolean following;
        private boolean allow_all_act_msg;
        private boolean geo_enabled;
        private boolean verified;
        private int verified_type;
        private String remark = "";
        private int ptype;
        private boolean allow_all_comment;
        private String avatar_large = "";
        private String avatar_hd = "";
        private String verified_reason = "";
        private int verified_trade;
        private String verified_reason_url = "";
        private String verified_source = "";
        private String verified_source_url = "";
        private int verified_state;
        private int verified_level;
        private String verified_reason_modified = "";
        private String verified_contact_name = "";
        private String verified_contact_email = "";
        private String verified_contact_mobile = "";
        private boolean follow_me;
        private int online_status;
        private int bi_followers_count;
        private String lang = "";
        private int star;
        private int mbtype;
        private int mbrank;
        private int block_word;
        private int block_app;
        private int credit_score;
        private int user_ability;
        private int urank;
        private Weibo weibo;
        private Date last_time;

        

        
        public Builder setLast_time(Date last_time) {
        
            this.last_time = last_time;
            return this;
        }

        public Builder(long id,  String screen_name, String name,Date created_at) {
            super();
            this.id = id;
            this.screen_name = screen_name;
            this.name = name;
            this.created_at=created_at;
        }

        public Builder setUclass(int uclass) {
            this.uclass = uclass;
            return this;
        }

        public Builder setProvince(int province) {
            this.province = province;
            return this;
        }

        public Builder setCity(int city) {
            this.city = city;
            return this;
        }

        public Builder setLocation(String location) {
            this.location = location;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setProfile_image_url(String profile_image_url) {
            this.profile_image_url = profile_image_url;
            return this;
        }

        public Builder setProfile_url(String profile_url) {
            this.profile_url = profile_url;
            return this;
        }

        public Builder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder setWeihao(String weihao) {
            this.weihao = weihao;
            return this;
        }

        public Builder setGender(String gender) {
            this.gender = gender;
            return this;
        }

        public Builder setFollowers_count(int followers_count) {
            this.followers_count = followers_count;
            return this;
        }

        public Builder setFriends_count(int friends_count) {
            this.friends_count = friends_count;
            return this;
        }

        public Builder setPagefriends_count(int pagefriends_count) {
            this.pagefriends_count = pagefriends_count;
            return this;
        }

        public Builder setStatuses_count(int statuses_count) {
            this.statuses_count = statuses_count;
            return this;
        }

        public Builder setFavourites_count(int favourites_count) {
            this.favourites_count = favourites_count;
            return this;
        }

        public Builder setFollowing(boolean following) {
            this.following = following;
            return this;
        }

        public Builder setAllow_all_act_msg(boolean allow_all_act_msg) {
            this.allow_all_act_msg = allow_all_act_msg;
            return this;
        }

        public Builder setGeo_enabled(boolean geo_enabled) {
            this.geo_enabled = geo_enabled;
            return this;
        }

        public Builder setVerified(boolean verified) {
            this.verified = verified;
            return this;
        }

        public Builder setVerified_type(int verified_type) {
            this.verified_type = verified_type;
            return this;
        }

        public Builder setRemark(String remark) {
            this.remark = remark;
            return this;
        }

        public Builder setPtype(int ptype) {
            this.ptype = ptype;
            return this;
        }

        public Builder setAllow_all_comment(boolean allow_all_comment) {
            this.allow_all_comment = allow_all_comment;
            return this;
        }

        public Builder setAvatar_large(String avatar_large) {
            this.avatar_large = avatar_large;
            return this;
        }

        public Builder setAvatar_hd(String avatar_hd) {
            this.avatar_hd = avatar_hd;
            return this;
        }

        public Builder setVerified_reason(String verified_reason) {
            this.verified_reason = verified_reason;
            return this;
        }

        public Builder setVerified_trade(int verified_trade) {
            this.verified_trade = verified_trade;
            return this;
        }

        public Builder setVerified_reason_url(String verified_reason_url) {
            this.verified_reason_url = verified_reason_url;
            return this;
        }

        public Builder setVerified_source(String verified_source) {
            this.verified_source = verified_source;
            return this;
        }

        public Builder setVerified_source_url(String verified_source_url) {
            this.verified_source_url = verified_source_url;
            return this;
        }

        public Builder setVerified_state(int verified_state) {
            this.verified_state = verified_state;
            return this;
        }

        public Builder setVerified_level(int verified_level) {
            this.verified_level = verified_level;
            return this;
        }

        public Builder setVerified_reason_modified(String verified_reason_modified) {
            this.verified_reason_modified = verified_reason_modified;
            return this;
        }

        public Builder setVerified_contact_name(String verified_contact_name) {
            this.verified_contact_name = verified_contact_name;
            return this;
        }

        public Builder setVerified_contact_email(String verified_contact_email) {
            this.verified_contact_email = verified_contact_email;
            return this;
        }

        public Builder setVerified_contact_mobile(String verified_contact_mobile) {
            this.verified_contact_mobile = verified_contact_mobile;
            return this;
        }

        public Builder setFollow_me(boolean follow_me) {
            this.follow_me = follow_me;
            return this;
        }

        public Builder setOnline_status(int online_status) {
            this.online_status = online_status;
            return this;
        }

        public Builder setBi_followers_count(int bi_followers_count) {
            this.bi_followers_count = bi_followers_count;
            return this;
        }

        public Builder setLang(String lang) {
            this.lang = lang;
            return this;
        }

        public Builder setStar(int star) {
            this.star = star;
            return this;
        }

        public Builder setMbtype(int mbtype) {
            this.mbtype = mbtype;
            return this;
        }

        public Builder setMbrank(int mbrank) {
            this.mbrank = mbrank;
            return this;
        }

        public Builder setBlock_word(int block_word) {
            this.block_word = block_word;
            return this;
        }

        public Builder setBlock_app(int block_app) {
            this.block_app = block_app;
            return this;
        }

        public Builder setCredit_score(int credit_score) {
            this.credit_score = credit_score;
            return this;
        }

        public Builder setUser_ability(int user_ability) {
            this.user_ability = user_ability;
            return this;
        }

        public Builder setUrank(int urank) {
            this.urank = urank;
            return this;
        }
        public Builder setWeibo(Weibo weibo){
            this.weibo=weibo;
            return this;
        }
        public User build() {
            return new User(this);
        }
        
    }

    @Override
    public String toString() {
        return "User [id=" + id + ", screen_name=" + screen_name
                + ", name=" + name + ", province=" + province + ", city=" + city + ", location=" + location
                + ", description=" + description + ", url=" + url + ", profile_image_url=" + profile_image_url
                + ", domain=" + domain + ", weihao=" + weihao + ", gender=" + gender
                + ", followers_count=" + followers_count + ", friends_count=" + friends_count +", favourites_count=" + favourites_count
                + ", created_at=" + created_at+",statuses_count="+statuses_count
                + ", verified=" + verified + ", verified_type="
                + verified_type + ", remark=" + remark + ", avatar_large=" + avatar_large 
                + ", verified_reason=" + verified_reason + ", bi_followers_count=" + bi_followers_count
                +"]";
    }

    

    
    public Date getLasttime() {
    
        return lasttime;
    }

    
    public void setLasttime(Date lasttime) {
    
        this.lasttime = lasttime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    public String getScreen_name() {
        return screen_name;
    }

    public void setScreen_name(String screen_name) {
        this.screen_name = screen_name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getProvince() {
        return province;
    }

    public void setProvince(int province) {
        this.province = province;
    }

    public int getCity() {
        return city;
    }

    public void setCity(int city) {
        this.city = city;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProfile_image_url() {
        return profile_image_url;
    }

    public void setProfile_image_url(String profile_image_url) {
        this.profile_image_url = profile_image_url;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getWeihao() {
        return weihao;
    }

    public void setWeihao(String weihao) {
        this.weihao = weihao;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getFollowers_count() {
        return followers_count;
    }

    public void setFollowers_count(int followers_count) {
        this.followers_count = followers_count;
    }

    public int getFriends_count() {
        return friends_count;
    }

    public void setFriends_count(int friends_count) {
        this.friends_count = friends_count;
    }


    public int getStatuses_count() {
        return statuses_count;
    }

    public void setStatuses_count(int statuses_count) {
        this.statuses_count = statuses_count;
    }

    public int getFavourites_count() {
        return favourites_count;
    }

    public void setFavourites_count(int favourites_count) {
        this.favourites_count = favourites_count;
    }

    public Date getCreated_at() {
        return created_at;
    }

    public void setCreated_at(Date created_at) {
        this.created_at = created_at;
    }


    public boolean isVerified() {
        return verified;
    }

    public void setVerified(boolean verified) {
        this.verified = verified;
    }

    public int getVerified_type() {
        return verified_type;
    }

    public void setVerified_type(int verified_type) {
        this.verified_type = verified_type;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }


    public String getAvatar_large() {
        return avatar_large;
    }

    public void setAvatar_large(String avatar_large) {
        this.avatar_large = avatar_large;
    }


    public String getVerified_reason() {
        return verified_reason;
    }

    public void setVerified_reason(String verified_reason) {
        this.verified_reason = verified_reason;
    }


    public int getBi_followers_count() {
        return bi_followers_count;
    }

    public void setBi_followers_count(int bi_followers_count) {
        this.bi_followers_count = bi_followers_count;
    }


}