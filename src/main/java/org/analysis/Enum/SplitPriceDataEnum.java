package org.analysis.Enum;

public enum SplitPriceDataEnum {
    SuperLow(" price <=10 "){
        @Override
        public int[] getValues() {
            return new int[]{10};
        }

        @Override
        public String getValue() {
            return "<=10";
        }
    },
    Low(" price >10 and price<=100 "){
        @Override
        public int[] getValues() {
            return new int[]{10,100};
        }
        @Override
        public String getValue() {
            return "10-100";
        }
    },
    MEDIUM(" price >100 and price<=500 "){
        @Override
        public int[] getValues() {
            return new int[]{100,500};
        }
        @Override
        public String getValue() {
            return "100-500";
        }
    },
    ABOVEMODERATE(" price > 500 and price <=2500"){
        @Override
        public int[] getValues() {
            return new int[]{500,2500};
        }
        @Override
        public String getValue() {
            return "500-2500";
        }
    },
    FINEST(" price >2500 and price <=7000 "){
        @Override
        public int[] getValues() {
            return new int[]{2500,7000};
        }
        @Override
        public String getValue() {
            return "2500-7000";
        }
    },
    HIGHER(" price >7000 and price <= 18000"){
        @Override
        public int[] getValues() {
            return new int[]{7000,18000};
        }
        @Override
        public String getValue() {
            return "7000-18000";
        }
    },
    HIGHIEST(" price >18000"){
        @Override
        public int[] getValues() {
            return new int[]{18000,18000};
        }
        @Override
        public String getValue() {
            return ">18000";
        }
    };

    public final String sql;

    SplitPriceDataEnum(String sql){
        this.sql = sql;
    }

    public String getSql(boolean and){
        return (and?" and"+sql:sql);
    }
    public abstract int[] getValues();

    public abstract String getValue();
}
