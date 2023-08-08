package org.analysis.Enum;

public enum SpliteDateEnum {
    FULL(" true "){

    },

    ONEMOUTH(" act_date >= '2018-03-15' "){

    },
    TWOWEEK(" act_date >= '2018-04-01' "){

    },
    WEEK(" act_date >= '2018-04-07' "){

    };

    public final String sql;
    SpliteDateEnum(String sql){
        this.sql = sql;
    }

    public String getSql(boolean and){
        return (and?" and"+sql:sql);
    }

    public static SpliteDateEnum getInstance(int time){
        if(time == 1){
            return FULL;
        }else if (time == 2) {
            return ONEMOUTH;
        }else if (time == 3) {
            return TWOWEEK;
        }else {
            return WEEK;
        }
    }
}
