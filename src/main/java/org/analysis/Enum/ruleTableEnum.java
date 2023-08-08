package org.analysis.Enum;

public enum ruleTableEnum {
    RULE_USER_TAG_ONE("rule_user_tag_one"){
        @Override
        public String getTableName() {
            return tableName;
        }
    },
    RULE_USER_TAG_TWO("rule_user_tag_two"){
        @Override
        public String getTableName() {
            return tableName;
        }
    },
    RULE_USER_TAG_THREE("rule_user_tag_three"){
        @Override
        public String getTableName() {
            return tableName;
        }
    },
    RULE_USER_TAG_FOUR("rule_user_tag_"){
        @Override
        public String getTableName() {
            return tableName;
        }
    };
    ruleTableEnum(String tableName){
        this.tableName = tableName;
    }

    public final String tableName;

    public abstract String getTableName();

    public static ruleTableEnum getRuleUserTagByTime(int time){
        switch (time){
            case 1:
                return RULE_USER_TAG_ONE;
            case 2:
                return RULE_USER_TAG_TWO;
            case 3:
                return RULE_USER_TAG_THREE;
            case 4:
                return RULE_USER_TAG_FOUR;
            default:
                return RULE_USER_TAG_ONE;
        }
    }
}
