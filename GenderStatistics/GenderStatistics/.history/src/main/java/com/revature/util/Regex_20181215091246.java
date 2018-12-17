package com.revature.Util;

/**
 * SetTheRegex
 */
public class Regex {

    private static String regex;
    private static String[] regexes;

    public void setRegex(String regex){this.regex=regex;}
    public void setRegexes(String ... regexes){this.regexes = regexes;}
    public String getRegex(){return this.regex;}
    public String[] getRegexes(){return this.regexes;}
    
}