package org.analysis.mllib.clv.Enum;

public enum rfmTagEnum {
    NEW_USER(1,"新客户"){},
    IMPORT_USER(2,"重要价值客户"){},
    COMMON_USER(3,"一般维持客户"){},

    POTENTIAL_USER(4,"潜力客户"){},

    DEEP_PLOWING_USER(5,"重要深耕客户"){},

    LOSING_USER(6,"流失客户"){},

    RECALL_USER(7,"重要唤回客户"){},

    RETAIN_USER(8,"重要挽留客户"){};

    public final int value;

    public final String tag;
    rfmTagEnum(int num,String tag){
        this.value = num;
        this.tag = tag;
    }

    public static rfmTagEnum getTagEnum(String tag){
        for(rfmTagEnum tagEnum : rfmTagEnum.values()){
            if(tagEnum.tag.equals(tag)){
                return tagEnum;
            }
        }
        return null;
    }

    public static rfmTagEnum getTagEnum(int num){
        for (rfmTagEnum tagEnum: rfmTagEnum.values()){
            if(tagEnum.value == num){
                return tagEnum;
            }
        }
        return null;
    }
}
