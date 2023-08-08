package org.analysis.Enum;

public enum SplitTimeDataEnum {

    DAWN(" act_time between '01:00:00' and '04:59:59' "){
        @Override
        public Integer getIdx() {
            return 0;
        }

        @Override
        public String  getValue() {
            return "凌晨";
        }
    },

    MORNGING(" act_time between '05:00:00' and '07:59:59' "){
        @Override
        public Integer getIdx() {
            return 1;
        }
        @Override
        public String  getValue() {
            return "早上";
        }

    },
    NOON(" act_time between '08:00:00' AND '10:59:59' "){
        @Override
        public Integer getIdx() {
            return 2;
        }

        @Override
        public String  getValue() {
            return "上午";
        }
    },
    MIDDAY(" act_time between '11:00:00' and '12:59:59' "){
        @Override
        public Integer getIdx() {
            return 3;
        }
        @Override
        public String  getValue() {
            return "中午";
        }
    },
    AFTERNOON(" act_time between '13:00:00' and '17:59:59' "){
        @Override
        public Integer getIdx() {
            return 4;
        }

        @Override
        public String  getValue() {
            return "下午";
        }
    },
    EVENING(" act_time between '18:00:00' AND '23:59:59' "){
        @Override
        public Integer getIdx() {
            return 5;
        }

        @Override
        public String  getValue() {
            return "晚上";
        }
    };

    public final String sql;
    SplitTimeDataEnum(String sql){
        this.sql = sql;
    }

    public String getSql(boolean and){
        return (and?" and"+sql:sql);
    }

    public abstract Integer getIdx();

    public abstract String getValue();
}
