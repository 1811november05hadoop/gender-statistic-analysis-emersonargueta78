package com.revature.util;

/**
 * SetTheRegex
 */
public interface Globals {

    // private static String regex;
    // private static String[] regexes;
    // private static String[] ranges;

    public void setRegex(String regex);//{this.regex=regex;}
    public void setRegexes(String ... regexes);//{this.regexes = regexes;}
    public String getRegex();//{return this.regex;}
    public String[] getRegexes();//{return this.regexes;}
    public void setYearlyRanges(String ...ranges);// {this.ranges=ranges;}
    public String[] getYearlyRanges();// {return this.ranges;}
}