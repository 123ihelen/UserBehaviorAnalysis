package com.jd.hotitem_analysis.beans;

/**
 * @author Walter
 * @create 2022-02-28-2:24 下午
 */
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timeStamp;

    public UserBehavior() {

    }

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timeStamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timeStamp = timeStamp;
    }

    public Long getUserId() {
        return userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
